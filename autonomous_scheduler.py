"""
Autonomous Scheduler for HailyDB v2.0
Implements self-running background operations without APScheduler
Uses timestamp-based triggering with overlap prevention
"""
import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Optional
from app import db
from models import SchedulerLog
from ingest import IngestService
from spc_ingest import SPCIngestService
from spc_matcher import SPCMatchingService
from scheduler_service import SchedulerService
from config import Config

logger = logging.getLogger(__name__)

class AutonomousScheduler:
    """
    Self-running scheduler that maintains autonomous ingestion
    Prevents overlapping operations and provides self-diagnosis
    """
    
    def __init__(self, db):
        self.db = db
        self.ingest_service = IngestService(db)
        self.spc_service = SPCIngestService(db.session)
        self.matching_service = SPCMatchingService(db.session)
        self.scheduler_service = SchedulerService(db)
        
        self.running = False
        self.thread = None
        
        # Scheduling intervals (minutes)
        self.nws_interval = Config.POLLING_INTERVAL_MINUTES  # 5 minutes
        self.spc_interval = 60  # 60 minutes
        self.matching_interval = 30  # 30 minutes
        
        # Last execution tracking
        self.last_nws_poll = None
        self.last_spc_poll = None
        self.last_matching = None
        
        # Operation locks to prevent overlaps
        self.nws_lock = threading.Lock()
        self.spc_lock = threading.Lock()
        self.matching_lock = threading.Lock()
        
        # Track last operation results for dashboard
        self.last_operation_result = None
    
    def start(self):
        """Start the autonomous scheduler"""
        if self.running:
            logger.warning("Scheduler already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.thread.start()
        logger.info("Autonomous scheduler started")
    
    def stop(self):
        """Stop the autonomous scheduler"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Autonomous scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop that runs continuously"""
        from app import app
        logger.info("Scheduler loop started")
        
        while self.running:
            try:
                with app.app_context():
                    current_time = datetime.utcnow()
                    
                    # Check if NWS polling is due
                    if self._should_run_nws_poll(current_time):
                        self._run_nws_poll()
                    
                    # Check if SPC polling is due
                    if self._should_run_spc_poll(current_time):
                        self._run_spc_poll()
                    
                    # Check if matching is due
                    if self._should_run_matching(current_time):
                        self._run_matching()
                
                # Sleep for 30 seconds before next check
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                time.sleep(60)  # Wait longer on errors
    
    def _should_run_nws_poll(self, current_time: datetime) -> bool:
        """Check if NWS polling should run - aligned to exact 5-minute intervals"""
        # Calculate minutes since the hour
        minutes_since_hour = current_time.minute
        seconds_since_minute = current_time.second
        
        # Check if we're at an exact 5-minute mark (0, 5, 10, 15, etc.)
        is_five_minute_mark = (minutes_since_hour % 5 == 0) and (seconds_since_minute < 30)
        
        # Only run if we're at a 5-minute mark AND haven't run recently
        if not is_five_minute_mark:
            return False
            
        if self.last_nws_poll is None:
            return True
        
        # Ensure we don't run multiple times within the same minute
        time_since_last = current_time - self.last_nws_poll
        return time_since_last.total_seconds() >= 240  # At least 4 minutes since last run
    
    def _should_run_spc_poll(self, current_time: datetime) -> bool:
        """Check if SPC polling should run"""
        if self.last_spc_poll is None:
            return True
        
        time_since_last = current_time - self.last_spc_poll
        return time_since_last.total_seconds() >= (self.spc_interval * 60)
    
    def _should_run_matching(self, current_time: datetime) -> bool:
        """Check if matching should run"""
        if self.last_matching is None:
            return True
        
        time_since_last = current_time - self.last_matching
        return time_since_last.total_seconds() >= (self.matching_interval * 60)
    
    def _run_nws_poll(self):
        """Execute NWS polling with overlap prevention"""
        if not self.nws_lock.acquire(blocking=False):
            logger.warning("NWS polling already in progress, skipping")
            return
        
        log_entry = None
        try:
            log_entry = self.scheduler_service.log_operation_start(
                "nws_poll", "internal_timer"
            )
            
            new_alerts = self.ingest_service.poll_nws_alerts()
            
            self.scheduler_service.log_operation_complete(
                log_entry, True, new_alerts, new_alerts
            )
            
            self.last_nws_poll = datetime.utcnow()
            self.last_operation_result = {
                'operation': 'nws',
                'success': True,
                'message': f'{new_alerts} new alerts ingested',
                'timestamp': self.last_nws_poll.isoformat()
            }
            logger.info(f"NWS polling completed: {new_alerts} new alerts")
            
        except Exception as e:
            logger.error(f"NWS polling failed: {e}")
            self.last_operation_result = {
                'operation': 'nws',
                'success': False,
                'message': f'NWS polling failed: {str(e)}',
                'timestamp': datetime.utcnow().isoformat()
            }
            if log_entry:
                self.scheduler_service.log_operation_complete(
                    log_entry, False, 0, 0, str(e)
                )
        finally:
            self.nws_lock.release()
    
    def _run_spc_poll(self):
        """Execute SPC polling with overlap prevention"""
        if not self.spc_lock.acquire(blocking=False):
            logger.warning("SPC polling already in progress, skipping")
            return
        
        log_entry = None
        try:
            log_entry = self.scheduler_service.log_operation_start(
                "spc_poll", "internal_timer"
            )
            
            # Systematic polling for T-0 through T-15
            today = datetime.utcnow().date()
            
            total_reports = 0
            for days_back in range(16):  # T-0 through T-15
                target_date = today - timedelta(days=days_back)
                
                # Check if this date should be polled based on systematic schedule
                # Only poll if we don't have recent successful data for this date
                if self.spc_service.should_poll_now(target_date):
                    try:
                        # Check if we already have fresh data for this date
                        from models import SPCReport
                        existing_count = SPCReport.query.filter(
                            SPCReport.report_date == target_date
                        ).count()
                        
                        # Skip polling if we already have substantial data for this date
                        if existing_count > 0 and days_back >= 7:
                            logger.debug(f"Skipping {target_date}: already has {existing_count} records")
                            continue
                        
                        result = self.spc_service.poll_spc_reports(target_date)
                        if result.get('status') != 'skipped':
                            total_reports += result.get('total_reports', 0)
                    except Exception as e:
                        logger.warning(f"Failed to poll {target_date}: {e}")
                        continue
            
            self.scheduler_service.log_operation_complete(
                log_entry, True, total_reports, total_reports
            )
            
            self.last_spc_poll = datetime.utcnow()
            logger.info(f"SPC polling completed: {total_reports} reports processed")
            
        except Exception as e:
            logger.error(f"SPC polling failed: {e}")
            if log_entry:
                self.scheduler_service.log_operation_complete(
                    log_entry, False, 0, 0, str(e)
                )
        finally:
            self.spc_lock.release()
    
    def _run_matching(self):
        """Execute SPC matching with overlap prevention"""
        if not self.matching_lock.acquire(blocking=False):
            logger.warning("SPC matching already in progress, skipping")
            return
        
        log_entry = None
        try:
            log_entry = self.scheduler_service.log_operation_start(
                "spc_match", "internal_timer"
            )
            
            result = self.matching_service.match_spc_reports_batch(limit=100)
            processed = result.get('processed', 0)
            matched = result.get('matched', 0)
            
            self.scheduler_service.log_operation_complete(
                log_entry, True, processed, matched
            )
            
            self.last_matching = datetime.utcnow()
            logger.info(f"SPC matching completed: {matched}/{processed} alerts matched")
            
        except Exception as e:
            logger.error(f"SPC matching failed: {e}")
            if log_entry:
                self.scheduler_service.log_operation_complete(
                    log_entry, False, 0, 0, str(e)
                )
        finally:
            self.matching_lock.release()
    
    def get_status(self) -> dict:
        """Get scheduler status for diagnostics"""
        # Get most recent NWS operation from database
        last_operation = None
        try:
            from models import SchedulerLog
            recent_nws_op = SchedulerLog.query.filter_by(
                operation_type='nws_poll'
            ).filter(SchedulerLog.completed_at.isnot(None)).order_by(SchedulerLog.completed_at.desc()).first()
            
            if recent_nws_op and recent_nws_op.completed_at:
                last_operation = {
                    'completed_at': recent_nws_op.completed_at.isoformat(),
                    'success': recent_nws_op.success,
                    'records_new': recent_nws_op.records_new or 0
                }
        except Exception as e:
            logger.warning(f"Could not fetch last operation: {e}")
        
        return {
            'running': self.running,
            'thread_alive': self.thread.is_alive() if self.thread else False,
            'last_nws_poll': self.last_nws_poll.isoformat() if self.last_nws_poll else None,
            'last_spc_poll': self.last_spc_poll.isoformat() if self.last_spc_poll else None,
            'last_matching': self.last_matching.isoformat() if self.last_matching else None,
            'intervals': {
                'nws_minutes': self.nws_interval,
                'spc_minutes': self.spc_interval,
                'matching_minutes': self.matching_interval
            },
            'last_operation': last_operation,
            'last_operation_result': self.last_operation_result
        }
    
    def force_run_all(self):
        """Force run all operations (for manual trigger)"""
        self._run_nws_poll()
        self._run_spc_poll()
        self._run_matching()

# Global scheduler instance
autonomous_scheduler = None

def init_scheduler(db_session):
    """Initialize the global scheduler instance"""
    global autonomous_scheduler
    autonomous_scheduler = AutonomousScheduler(db_session)
    return autonomous_scheduler

def start_scheduler():
    """Start the global scheduler"""
    if autonomous_scheduler:
        autonomous_scheduler.start()

def stop_scheduler():
    """Stop the global scheduler"""
    if autonomous_scheduler:
        autonomous_scheduler.stop()

def get_scheduler_status():
    """Get scheduler status"""
    if autonomous_scheduler:
        return autonomous_scheduler.get_status()
    return {'running': False, 'error': 'Scheduler not initialized'}