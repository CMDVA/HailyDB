import logging
import requests
from datetime import datetime
from typing import Optional, Dict, List
from models import Alert, IngestionLog
from config import Config

logger = logging.getLogger(__name__)

class IngestService:
    """
    NWS Alert Ingestion Service
    Handles polling, pagination, deduplication, and storage
    """
    
    def __init__(self, db):
        self.db = db
        self.config = Config()
        
    def poll_nws_alerts(self) -> int:
        """
        Main ingestion method - polls NWS API and stores alerts
        Returns number of new alerts ingested
        """
        log_entry = IngestionLog()
        self.db.session.add(log_entry)
        self.db.session.commit()
        
        try:
            logger.info("Starting NWS alert ingestion")
            
            total_processed = 0
            new_alerts = 0
            updated_alerts = 0
            
            url = self.config.NWS_ALERT_URL
            
            while url:
                logger.debug(f"Polling URL: {url}")
                
                try:
                    response = requests.get(url, headers=self.config.NWS_HEADERS, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    features = data.get('features', [])
                    
                    logger.info(f"Retrieved {len(features)} alerts from current page")
                    
                    for feature in features:
                        try:
                            result = self._process_alert_feature(feature)
                            total_processed += 1
                            
                            if result == 'new':
                                new_alerts += 1
                            elif result == 'updated':
                                updated_alerts += 1
                                
                        except Exception as e:
                            logger.error(f"Error processing alert feature: {e}")
                            continue
                    
                    # Handle pagination
                    pagination = data.get('pagination', {})
                    url = pagination.get('next')
                    
                    if url:
                        logger.debug(f"Following pagination to: {url}")
                    
                except requests.RequestException as e:
                    logger.error(f"HTTP error during ingestion: {e}")
                    break
                except Exception as e:
                    logger.error(f"Unexpected error during page processing: {e}")
                    break
            
            # Commit all changes
            self.db.session.commit()
            
            # Update log entry
            log_entry.completed_at = datetime.utcnow()
            log_entry.success = True
            log_entry.alerts_processed = total_processed
            log_entry.new_alerts = new_alerts
            log_entry.updated_alerts = updated_alerts
            self.db.session.commit()
            
            logger.info(f"Ingestion complete: {new_alerts} new, {updated_alerts} updated, {total_processed} total")
            return new_alerts
            
        except Exception as e:
            logger.error(f"Fatal error during ingestion: {e}")
            
            # Update log entry with error
            log_entry.completed_at = datetime.utcnow()
            log_entry.success = False
            log_entry.error_message = str(e)
            self.db.session.commit()
            
            raise
    
    def _process_alert_feature(self, feature: Dict) -> str:
        """
        Process a single alert feature from NWS API
        Returns 'new', 'updated', or 'skipped'
        """
        try:
            properties = feature.get('properties', {})
            alert_id = properties.get('id')
            
            if not alert_id:
                logger.warning("Alert feature missing ID, skipping")
                return 'skipped'
            
            # Check if alert already exists
            existing_alert = Alert.query.get(alert_id)
            
            if existing_alert:
                # Update existing alert
                self._update_alert(existing_alert, feature)
                logger.debug(f"Updated existing alert: {alert_id}")
                return 'updated'
            else:
                # Create new alert
                self._create_alert(feature)
                logger.debug(f"Created new alert: {alert_id}")
                return 'new'
                
        except Exception as e:
            logger.error(f"Error processing alert feature: {e}")
            raise
    
    def _create_alert(self, feature: Dict) -> Alert:
        """Create new alert from NWS feature"""
        properties = feature.get('properties', {})
        
        alert = Alert(
            id=properties.get('id'),
            event=properties.get('event'),
            severity=properties.get('severity'),
            area_desc=properties.get('areaDesc'),
            effective=self._parse_datetime(properties.get('effective')),
            expires=self._parse_datetime(properties.get('expires')),
            sent=self._parse_datetime(properties.get('sent')),
            geometry=feature.get('geometry'),
            properties=properties,
            raw=feature
        )
        
        self.db.session.add(alert)
        return alert
    
    def _update_alert(self, alert: Alert, feature: Dict) -> Alert:
        """Update existing alert with new data"""
        properties = feature.get('properties', {})
        
        # Update fields that might change
        alert.event = properties.get('event')
        alert.severity = properties.get('severity')
        alert.area_desc = properties.get('areaDesc')
        alert.effective = self._parse_datetime(properties.get('effective'))
        alert.expires = self._parse_datetime(properties.get('expires'))
        alert.sent = self._parse_datetime(properties.get('sent'))
        alert.geometry = feature.get('geometry')
        alert.properties = properties
        alert.raw = feature
        
        return alert
    
    def _parse_datetime(self, dt_string: Optional[str]) -> Optional[datetime]:
        """Parse ISO datetime string from NWS API"""
        if not dt_string:
            return None
        
        try:
            # Handle various ISO formats
            if dt_string.endswith('Z'):
                dt_string = dt_string[:-1] + '+00:00'
            
            return datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        except ValueError as e:
            logger.warning(f"Could not parse datetime '{dt_string}': {e}")
            return None
    
    def get_ingestion_stats(self) -> Dict:
        """Get ingestion statistics"""
        recent_logs = IngestionLog.query.order_by(
            IngestionLog.started_at.desc()
        ).limit(10).all()
        
        successful_runs = sum(1 for log in recent_logs if log.success)
        total_runs = len(recent_logs)
        
        latest_log = recent_logs[0] if recent_logs else None
        
        return {
            'recent_success_rate': successful_runs / total_runs if total_runs > 0 else 0,
            'total_recent_runs': total_runs,
            'latest_run': {
                'started_at': latest_log.started_at.isoformat() if latest_log else None,
                'success': latest_log.success if latest_log else None,
                'new_alerts': latest_log.new_alerts if latest_log else 0,
                'alerts_processed': latest_log.alerts_processed if latest_log else 0
            } if latest_log else None
        }
