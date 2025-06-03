"""
Scheduler Service for HailyDB v2.0 Core Upgrade
Provides operation logging and error recovery for autonomous ingestion
"""
import logging
from datetime import datetime
from typing import Dict, Optional
from app import db
from models import SchedulerLog

logger = logging.getLogger(__name__)

class SchedulerService:
    """
    Service to log and track all ingestion operations
    Maintains audit trail for autonomous system monitoring
    """
    
    def __init__(self, db):
        self.db = db
    
    def log_operation_start(self, operation_type: str, trigger_method: str = "manual", 
                          metadata: Optional[Dict] = None) -> SchedulerLog:
        """
        Start logging an operation
        Returns the log entry for completion tracking
        """
        try:
            log_entry = SchedulerLog(
                operation_type=operation_type,
                trigger_method=trigger_method,
                started_at=datetime.utcnow(),
                operation_metadata=metadata or {}
            )
            
            self.db.session.add(log_entry)
            self.db.session.commit()
            
            logger.info(f"Started operation {operation_type} (trigger: {trigger_method})")
            return log_entry
            
        except Exception as e:
            logger.error(f"Failed to log operation start: {e}")
            self.db.session.rollback()
            raise
    
    def log_operation_complete(self, log_entry: SchedulerLog, success: bool,
                             records_processed: int = 0, records_new: int = 0,
                             error_message: str = None):
        """
        Complete an operation log entry
        """
        try:
            log_entry.completed_at = datetime.utcnow()
            log_entry.success = success
            log_entry.records_processed = records_processed
            log_entry.records_new = records_new
            
            if error_message:
                log_entry.error_message = error_message
            
            self.db.session.commit()
            
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"Completed operation {log_entry.operation_type}: {status} "
                       f"(processed: {records_processed}, new: {records_new})")
            
        except Exception as e:
            logger.error(f"Failed to complete operation log: {e}")
            self.db.session.rollback()
            raise
    
    def get_recent_operations(self, hours: int = 24, operation_type: str = None) -> list:
        """
        Get recent operation logs for monitoring
        """
        from datetime import timedelta
        
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        query = SchedulerLog.query.filter(SchedulerLog.started_at >= cutoff)
        
        if operation_type:
            query = query.filter(SchedulerLog.operation_type == operation_type)
        
        return query.order_by(SchedulerLog.started_at.desc()).all()
    
    def get_operation_stats(self) -> Dict:
        """
        Get operation statistics for health monitoring
        """
        from datetime import timedelta
        
        cutoff_24h = datetime.utcnow() - timedelta(hours=24)
        
        recent_logs = SchedulerLog.query.filter(
            SchedulerLog.started_at >= cutoff_24h
        ).all()
        
        stats = {
            'total_operations_24h': len(recent_logs),
            'successful_operations_24h': len([log for log in recent_logs if log.success]),
            'failed_operations_24h': len([log for log in recent_logs if not log.success]),
            'operations_by_type': {},
            'last_successful_operations': {}
        }
        
        # Group by operation type
        for log in recent_logs:
            op_type = log.operation_type
            if op_type not in stats['operations_by_type']:
                stats['operations_by_type'][op_type] = {'total': 0, 'successful': 0, 'failed': 0}
            
            stats['operations_by_type'][op_type]['total'] += 1
            if log.success:
                stats['operations_by_type'][op_type]['successful'] += 1
            else:
                stats['operations_by_type'][op_type]['failed'] += 1
        
        # Last successful operation for each type
        for op_type in ['nws_poll', 'spc_poll', 'spc_match', 'ai_enrich']:
            last_success = SchedulerLog.query.filter(
                SchedulerLog.operation_type == op_type,
                SchedulerLog.success == True
            ).order_by(SchedulerLog.completed_at.desc()).first()
            
            if last_success:
                stats['last_successful_operations'][op_type] = last_success.completed_at.isoformat()
        
        return stats
    
    def cleanup_old_logs(self, days_to_keep: int = 30):
        """
        Clean up old operation logs to prevent database bloat
        """
        from datetime import timedelta
        
        cutoff = datetime.utcnow() - timedelta(days=days_to_keep)
        
        deleted_count = SchedulerLog.query.filter(
            SchedulerLog.started_at < cutoff
        ).delete()
        
        self.db.session.commit()
        logger.info(f"Cleaned up {deleted_count} old scheduler logs (older than {days_to_keep} days)")
        
        return deleted_count