"""
SPC Data Integrity Verification Service
Compares HailyDB SPC report counts with live SPC data to verify database correctness
"""

import requests
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple
import logging
from config import Config

logger = logging.getLogger(__name__)

class SPCVerificationService:
    """
    Service to verify SPC data integrity by comparing database counts with live SPC data
    """
    
    def __init__(self, db_session):
        self.db = db_session
        self.base_url = "https://www.spc.noaa.gov/climo/reports/"
        
    def verify_date_range(self, start_date: date, end_date: date) -> List[Dict]:
        """
        Verify SPC data integrity for a date range
        Returns list of verification results with format:
        {date, hailydb_count, spc_live_count, match_status}
        """
        verification_results = []
        current_date = start_date
        
        while current_date <= end_date:
            result = self.verify_single_date(current_date)
            verification_results.append(result)
            current_date += timedelta(days=1)
            
        return verification_results
    
    def verify_single_date(self, check_date: date) -> Dict:
        """
        Verify SPC data for a single date
        """
        from models import SPCReport
        
        # Get count from HailyDB
        hailydb_count = self.db.query(SPCReport).filter(
            SPCReport.report_date == check_date
        ).count()
        
        # Get count from live SPC data
        spc_live_count = self.get_live_spc_count(check_date)
        
        # Determine match status
        if spc_live_count is None:
            match_status = "SPC_UNAVAILABLE"
        elif hailydb_count == spc_live_count:
            match_status = "MATCH"
        else:
            match_status = "MISMATCH"
            
        return {
            'date': check_date.strftime('%Y-%m-%d'),
            'hailydb_count': hailydb_count,
            'spc_live_count': spc_live_count,
            'match_status': match_status,
            'difference': (spc_live_count - hailydb_count) if spc_live_count is not None else None
        }
    
    def get_live_spc_count(self, check_date: date) -> int:
        """
        Fetch live SPC report count for a specific date
        Returns total count across all report types (tornado, wind, hail)
        """
        try:
            # Format date for SPC URL (YYMMDD) - use filtered data to match ingestion
            date_str = check_date.strftime('%y%m%d')
            url = f"{self.base_url}{date_str}_rpts_filtered.csv"
            
            response = requests.get(url, timeout=30)
            if response.status_code == 404:
                # No data available for this date
                return 0
            elif response.status_code != 200:
                logger.warning(f"Failed to fetch SPC data for {check_date}: HTTP {response.status_code}")
                return None
                
            # Parse CSV content to count total reports
            csv_content = response.text
            total_count = self._count_reports_in_csv(csv_content)
            
            return total_count
            
        except Exception as e:
            logger.error(f"Error fetching live SPC data for {check_date}: {e}")
            return None
    
    def _count_reports_in_csv(self, csv_content: str) -> int:
        """
        Count total reports in SPC CSV content
        Properly handles the 3-section format: tornado, wind, hail
        """
        lines = csv_content.strip().split('\n')
        total_count = 0
        
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
                
            # Skip header lines that start with "Time," (column headers)
            if line.startswith('Time,'):
                continue
                
            # Check if this is a data line by looking for time format (4 digits) at start
            if ',' in line:
                fields = line.split(',')
                if len(fields) >= 6:  # All sections have at least 6 columns
                    first_field = fields[0].strip()
                    # Valid data lines start with 4-digit time (like "2037", "1700", etc.)
                    if first_field.isdigit() and len(first_field) == 4:
                        total_count += 1
            
        return total_count
    
    def get_verification_summary(self, results: List[Dict]) -> Dict:
        """
        Generate summary statistics from verification results
        """
        if not results:
            return {'total_dates': 0, 'matches': 0, 'mismatches': 0, 'unavailable': 0}
            
        matches = sum(1 for r in results if r['match_status'] == 'MATCH')
        mismatches = sum(1 for r in results if r['match_status'] == 'MISMATCH')
        unavailable = sum(1 for r in results if r['match_status'] == 'SPC_UNAVAILABLE')
        
        return {
            'total_dates': len(results),
            'matches': matches,
            'mismatches': mismatches,
            'unavailable': unavailable,
            'match_percentage': (matches / len(results)) * 100 if results else 0
        }
    
    def trigger_reupload_for_date(self, check_date: date) -> Dict:
        """
        Trigger re-upload of SPC data for a specific date when mismatch is detected
        Deletes existing data for the date first, then re-ingests
        """
        from spc_ingest import SPCIngestService
        from models import SPCReport, SPCIngestionLog
        
        # Use a fresh database session to avoid transaction conflicts
        from app import db
        
        try:
            # Use ORM-based deletion to avoid session conflicts
            from models import SPCReport, SPCIngestionLog
            
            # Count existing reports first
            existing_count = self.db.query(SPCReport).filter(
                SPCReport.report_date == check_date
            ).count()
            
            # Delete existing reports for this date
            deleted_count = self.db.query(SPCReport).filter(
                SPCReport.report_date == check_date
            ).delete()
            
            # Also clear ingestion logs
            self.db.query(SPCIngestionLog).filter(
                SPCIngestionLog.report_date == check_date
            ).delete()
            
            # Commit deletions
            self.db.commit()
            logger.info(f"Deleted {deleted_count} existing SPC reports for {check_date}")
            
            # Now re-ingest the data using reimport method to bypass duplicate detection
            # Use fresh session to avoid transaction conflicts
            from app import db as fresh_db
            spc_ingester = SPCIngestService(fresh_db.session)
            result = spc_ingester.reimport_spc_reports(check_date)
            
            return {
                'success': True,
                'date': check_date.strftime('%Y-%m-%d'),
                'message': f"Re-uploaded SPC data for {check_date} (replaced {existing_count} existing reports)",
                'reports_ingested': result.get('total_reports', 0),
                'reports_replaced': existing_count
            }
            
        except Exception as e:
            # Rollback any partial changes
            try:
                self.db.rollback()
            except:
                pass
                
            logger.error(f"Error re-uploading SPC data for {check_date}: {e}")
            return {
                'success': False,
                'date': check_date.strftime('%Y-%m-%d'),
                'message': f"Failed to re-upload: {str(e)}"
            }