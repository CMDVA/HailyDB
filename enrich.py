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
        """Generate AI summary from alert description"""
        try:
            description = alert.properties.get('description', '')
            if not description:
                return None
            
            # Prepare prompt
            prompt = f"""
            Please provide a concise 2-3 sentence summary of this weather alert.
            Focus on the key hazards, affected areas, and timing.
            
            Alert Event: {alert.event}
            Area: {alert.area_desc}
            Description: {description[:1000]}
            """
            
            # the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
            # do not change this unless explicitly requested by the user
            response = self.openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a weather expert. Provide clear, concise summaries of weather alerts that help people understand the key risks and actions needed."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=200,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Error generating summary for alert {alert.id}: {e}")
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
    
    def get_enrichment_stats(self) -> Dict:
        """Get enrichment statistics"""
        try:
            total_alerts = Alert.query.count()
            enriched_alerts = Alert.query.filter(Alert.ai_summary.isnot(None)).count()
            tagged_alerts = Alert.query.filter(Alert.ai_tags.isnot(None)).count()
            
            return {
                'total_alerts': total_alerts,
                'enriched_alerts': enriched_alerts,
                'tagged_alerts': tagged_alerts,
                'enrichment_rate': enriched_alerts / total_alerts if total_alerts > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error getting enrichment stats: {e}")
            return {
                'total_alerts': 0,
                'enriched_alerts': 0,
                'tagged_alerts': 0,
                'enrichment_rate': 0
            }
