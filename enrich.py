import os
import json
import logging
from typing import Optional, List, Dict
from openai import OpenAI
from models import Alert

logger = logging.getLogger(__name__)

class EnrichmentService:
    """
    AI-powered alert enrichment service
    Provides summarization and tag classification using OpenAI
    """
    
    # High-priority alert types for automatic enrichment
    AUTO_ENRICH_ALERTS = {
        # Severe Weather Alert category
        'Tornado Watch', 'Tornado Warning', 'Severe Thunderstorm Watch', 
        'Severe Thunderstorm Warning', 'Severe Weather Statement', 
        'Extreme Wind Warning', 'Snow Squall Warning',
        # Tropical Weather Alert category
        'Tropical Storm Watch', 'Tropical Storm Warning', 'Hurricane Watch',
        'Hurricane Warning', 'Storm Surge Watch', 'Storm Surge Warning',
        # Specific high wind alerts
        'High Wind Watch', 'High Wind Warning'
    }
    
    # Category mapping for batch processing
    CATEGORY_MAPPING = {
        'Severe Weather Alert': [
            'Tornado Watch', 'Tornado Warning', 'Severe Thunderstorm Watch', 
            'Severe Thunderstorm Warning', 'Severe Weather Statement', 
            'Extreme Wind Warning', 'Snow Squall Warning'
        ],
        'Winter Weather Alert': [
            'Winter Storm Watch', 'Winter Storm Warning', 'Blizzard Warning',
            'Ice Storm Warning', 'Winter Weather Advisory', 'Freezing Rain Advisory',
            'Wind Chill Advisory', 'Wind Chill Warning', 'Frost Advisory', 'Freeze Warning'
        ],
        'Flood Alert': [
            'Flood Watch', 'Flood Warning', 'Flash Flood Watch', 
            'Flash Flood Warning', 'Flood Advisory'
        ],
        'Coastal Alert': [
            'Coastal Flood Watch', 'Coastal Flood Warning', 'Coastal Flood Advisory',
            'Lakeshore Flood Watch', 'Lakeshore Flood Warning', 'Lakeshore Flood Advisory',
            'Beach Hazards Statement'
        ],
        'Wind & Fog Alert': [
            'High Wind Watch', 'High Wind Warning', 'Wind Advisory',
            'Dense Fog Advisory', 'Freezing Fog Advisory'
        ],
        'Fire Weather Alert': [
            'Fire Weather Watch', 'Red Flag Warning'
        ],
        'Air Quality & Dust Alert': [
            'Air Quality Alert', 'Air Stagnation Advisory', 'Blowing Dust Advisory',
            'Dust Storm Warning', 'Ashfall Advisory', 'Ashfall Warning'
        ],
        'Marine Alert': [
            'Small Craft Advisory', 'Gale Watch', 'Gale Warning', 'Storm Watch',
            'Storm Warning', 'Hurricane Force Wind Warning', 'Special Marine Warning',
            'Low Water Advisory', 'Brisk Wind Advisory', 'Marine Weather Statement',
            'Hazardous Seas Warning'
        ],
        'Tropical Weather Alert': [
            'Tropical Storm Watch', 'Tropical Storm Warning', 'Hurricane Watch',
            'Hurricane Warning', 'Storm Surge Watch', 'Storm Surge Warning'
        ],
        'Tsunami Alert': [
            'Tsunami Watch', 'Tsunami Advisory', 'Tsunami Warning'
        ],
        'General Weather Info': [
            'Special Weather Statement', 'Hazardous Weather Outlook', 'Short Term Forecast',
            'Public Information Statement', 'Administrative Message', 'Test Message'
        ]
    }
    
    def __init__(self, db):
        self.db = db
        self.openai_client = OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY")
        )
        
    def enrich_alert(self, alert: Alert) -> bool:
        """
        Enrich a single alert with AI summary and tags
        Returns True if successful, False otherwise
        """
        try:
            if not alert.properties:
                logger.warning(f"Alert {alert.id} has no properties to enrich")
                return False
            
            # Generate AI summary
            summary = self._generate_summary(alert)
            if summary:
                alert.ai_summary = summary
            
            # Generate tags
            tags = self._classify_tags(alert)
            if tags:
                alert.ai_tags = tags
            
            logger.info(f"Successfully enriched alert {alert.id}")
            return True
            
        except Exception as e:
            logger.error(f"Error enriching alert {alert.id}: {e}")
            return False
    
    def _generate_summary(self, alert: Alert) -> Optional[str]:
        """Generate AI summary from alert description with retry logic"""
        import time
        
        description = alert.properties.get('description', '')
        if not description:
            return None
        
        # Enhanced prompt with specific instructions for better summaries
        prompt = f"""
        Summarize this weather alert in 2-3 clear sentences that focus on:
        1. What hazard is happening (wind speeds, hail size, tornado threat, etc.)
        2. Where it's affecting (specific counties/cities)
        3. When it's valid until and movement direction if applicable
        
        Keep the language accessible and actionable for the public.
        
        Alert Type: {alert.event}
        Severity: {alert.severity}
        Areas Affected: {alert.area_desc}
        Alert Details: {description[:1200]}
        """
        
        # Retry logic for network issues
        for attempt in range(3):
            try:
                # the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
                # do not change this unless explicitly requested by the user
                response = self.openai_client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a professional meteorologist creating public safety summaries. Write clear, actionable weather alert summaries that help people understand immediate risks and protective actions. Focus on specific hazards, locations, and timing. Avoid jargon and keep language accessible to the general public."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    max_tokens=250,
                    temperature=0.2,
                    timeout=30
                )
                
                return response.choices[0].message.content.strip()
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for alert {alert.id}: {e}")
                if attempt < 2:  # Don't sleep on the last attempt
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"All retry attempts failed for alert {alert.id}: {e}")
                    return None
    
    def _classify_tags(self, alert: Alert) -> Optional[List[str]]:
        """Classify alert into standardized tags"""
        try:
            event = alert.event or ''
            description = alert.properties.get('description', '')
            severity = alert.severity or ''
            
            # Prepare classification prompt
            prompt = f"""
            Classify this weather alert into relevant tags. Choose from these categories:
            - Severe Weather (tornado, severe-thunderstorm, hail, wind)
            - Flooding (flood, flash-flood, coastal-flood)
            - Winter Weather (winter-storm, ice-storm, blizzard, snow)
            - Fire Weather (fire-weather, red-flag)
            - Marine Weather (marine-warning, small-craft)
            - Air Quality (air-quality, smoke)
            - Hurricane/Tropical (hurricane, tropical-storm, storm-surge)
            - Heat/Cold (heat, excessive-heat, cold, freeze)
            - Other hazards
            
            Alert Event: {event}
            Severity: {severity}
            Description: {description[:500]}
            
            Return only a JSON array of relevant tag strings.
            """
            
            # the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
            # do not change this unless explicitly requested by the user
            response = self.openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a weather classification expert. Return only valid JSON arrays of relevant weather tags."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                response_format={"type": "json_object"},
                max_tokens=100,
                temperature=0.1
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Extract tags from various possible JSON structures
            tags = result.get('tags', [])
            if not tags and isinstance(result, list):
                tags = result
            
            # Validate and clean tags
            if isinstance(tags, list):
                cleaned_tags = [tag.lower().strip() for tag in tags if isinstance(tag, str)]
                return cleaned_tags[:10]  # Limit to 10 tags max
            
            return []
            
        except Exception as e:
            logger.error(f"Error classifying tags for alert {alert.id}: {e}")
            return []
    
    def enrich_batch(self, limit: int = 50) -> Dict[str, int]:
        """
        Enrich a batch of unenriched alerts
        Returns statistics about the enrichment process
        """
        try:
            # Get alerts that haven't been enriched yet
            alerts = Alert.query.filter(
                Alert.ai_summary.is_(None)
            ).limit(limit).all()
            
            enriched_count = 0
            failed_count = 0
            
            for alert in alerts:
                if self.enrich_alert(alert):
                    enriched_count += 1
                else:
                    failed_count += 1
            
            # Commit changes
            self.db.session.commit()
            
            logger.info(f"Batch enrichment complete: {enriched_count} enriched, {failed_count} failed")
            
            return {
                'enriched': enriched_count,
                'failed': failed_count,
                'total_processed': len(alerts)
            }
            
        except Exception as e:
            logger.error(f"Error during batch enrichment: {e}")
            return {'enriched': 0, 'failed': 0, 'total_processed': 0}
    
    def enrich_by_category(self, category: str, limit: int = 100) -> Dict[str, int]:
        """
        Enrich alerts by category with timeout protection
        Returns statistics about the enrichment process
        """
        try:
            if category not in self.CATEGORY_MAPPING:
                logger.error(f"Unknown category: {category}")
                return {'error': f'Unknown category: {category}'}
            
            # Get unenriched alerts for the specified category
            alert_types = self.CATEGORY_MAPPING[category]
            alerts = Alert.query.filter(
                Alert.ai_summary.is_(None),
                Alert.event.in_(alert_types)
            ).limit(limit).all()
            
            enriched_count = 0
            failed_count = 0
            
            logger.info(f"Starting category enrichment for '{category}': {len(alerts)} alerts to process")
            
            # Process in smaller batches to prevent timeouts
            batch_size = 10
            for i in range(0, len(alerts), batch_size):
                batch = alerts[i:i + batch_size]
                
                for alert in batch:
                    try:
                        if self.enrich_alert(alert):
                            enriched_count += 1
                        else:
                            failed_count += 1
                    except Exception as e:
                        logger.error(f"Error enriching alert {alert.id}: {e}")
                        failed_count += 1
                
                # Commit after each batch
                try:
                    self.db.session.commit()
                    logger.info(f"Category '{category}' batch {i//batch_size + 1}: {enriched_count} enriched so far")
                except Exception as e:
                    logger.error(f"Error committing batch: {e}")
                    self.db.session.rollback()
            
            logger.info(f"Category enrichment complete for '{category}': {enriched_count} enriched, {failed_count} failed")
            
            return {
                'category': category,
                'enriched': enriched_count,
                'failed': failed_count,
                'total_processed': len(alerts)
            }
            
        except Exception as e:
            logger.error(f"Error during category enrichment for {category}: {e}")
            self.db.session.rollback()
            return {
                'category': category,
                'enriched': 0,
                'failed': 0,
                'total_processed': 0,
                'error': str(e)
            }
    
    def should_auto_enrich(self, alert: Alert) -> bool:
        """Check if alert should be automatically enriched on ingestion"""
        return alert.event in self.AUTO_ENRICH_ALERTS
    
    def enrich_all_priority_alerts(self) -> Dict[str, int]:
        """
        Enrich all high-priority alerts that haven't been enriched yet
        Includes Severe Weather, Tropical Weather, and High Wind alerts
        """
        import time
        
        try:
            # Get all unenriched alerts that match auto-enrich criteria
            alerts = Alert.query.filter(
                Alert.ai_summary.is_(None),
                Alert.event.in_(self.AUTO_ENRICH_ALERTS)
            ).limit(5).all()  # Process only 5 at a time for stability
            
            total_enriched = 0
            total_failed = 0
            
            logger.info(f"Starting priority alert enrichment: {len(alerts)} alerts to process")
            
            if len(alerts) == 0:
                return {
                    'enriched': 0,
                    'failed': 0,
                    'total_processed': 0,
                    'message': 'No priority alerts need enrichment'
                }
            
            # Process alerts slowly and safely
            for i, alert in enumerate(alerts):
                try:
                    logger.info(f"Processing alert {i+1}/{len(alerts)}: {alert.id} ({alert.event})")
                    
                    if self.enrich_alert(alert):
                        total_enriched += 1
                        logger.info(f"Successfully enriched alert {alert.id}")
                        
                        # Commit immediately after success
                        try:
                            self.db.session.commit()
                        except Exception as e:
                            logger.error(f"Error committing alert {alert.id}: {e}")
                            self.db.session.rollback()
                            total_failed += 1
                            total_enriched = max(0, total_enriched - 1)
                    else:
                        total_failed += 1
                        logger.warning(f"Failed to enrich alert {alert.id}")
                    
                    # Wait between each alert to prevent overwhelming the API
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error enriching priority alert {alert.id}: {e}")
                    total_failed += 1
                    try:
                        self.db.session.rollback()
                    except:
                        pass
            
            logger.info(f"Priority alert enrichment complete: {total_enriched} enriched, {total_failed} failed")
            
            return {
                'enriched': total_enriched,
                'failed': total_failed,
                'total_processed': len(alerts)
            }
            
        except Exception as e:
            logger.error(f"Error during priority alert enrichment: {e}")
            self.db.session.rollback()
            return {
                'enriched': 0,
                'failed': 0,
                'total_processed': 0,
                'error': str(e)
            }
    
    def get_enrichment_stats(self) -> Dict:
        """Get enrichment statistics"""
        try:
            total_alerts = Alert.query.count()
            enriched_alerts = Alert.query.filter(Alert.ai_summary.isnot(None)).count()
            tagged_alerts = Alert.query.filter(Alert.ai_tags.isnot(None)).count()
            
            # Get priority alert stats
            priority_total = Alert.query.filter(Alert.event.in_(self.AUTO_ENRICH_ALERTS)).count()
            priority_enriched = Alert.query.filter(
                Alert.event.in_(self.AUTO_ENRICH_ALERTS),
                Alert.ai_summary.isnot(None)
            ).count()
            
            return {
                'total_alerts': total_alerts,
                'enriched_alerts': enriched_alerts,
                'tagged_alerts': tagged_alerts,
                'enrichment_rate': round((enriched_alerts / total_alerts * 100), 2) if total_alerts > 0 else 0,
                'priority_alerts_total': priority_total,
                'priority_alerts_enriched': priority_enriched,
                'priority_enrichment_rate': round((priority_enriched / priority_total * 100), 2) if priority_total > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error getting enrichment stats: {e}")
            return {
                'total_alerts': 0,
                'enriched_alerts': 0,
                'tagged_alerts': 0,
                'enrichment_rate': 0,
                'priority_alerts_total': 0,
                'priority_alerts_enriched': 0,
                'priority_enrichment_rate': 0
            }
