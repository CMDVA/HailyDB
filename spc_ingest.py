"""
Storm Prediction Center (SPC) Report Ingestion Service
Handles parsing of multi-section CSV files and cross-referencing with NWS alerts
"""

import csv
import logging
import requests
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from io import StringIO
import re

from config import Config
from models import SPCReport, SPCIngestionLog, Alert, db
from sqlalchemy import and_, or_, func
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)

class SPCIngestService:
    """
    SPC Report Ingestion Service
    Handles variable polling schedules and CSV parsing with header detection
    """
    
    def __init__(self, db_session):
        self.db = db_session
        self.base_url = "https://www.spc.noaa.gov/climo/reports/"
        
    def get_polling_schedule(self, report_date: date) -> int:
        """
        Determine polling interval based on date
        Returns interval in minutes
        """
        today = date.today()
        days_ago = (today - report_date).days
        
        if days_ago == 0:
            return 5  # Every 5 minutes for today
        elif days_ago <= 3:
            return 180  # Every 3 hours for recent (today-3)
        else:
            return 1440  # Daily (24 hours) for historical
    
    def should_poll_now(self, report_date: date) -> bool:
        """
        Check if we should poll for this date based on schedule
        """
        # Get last successful ingestion
        last_log = SPCIngestionLog.query.filter(
            SPCIngestionLog.report_date == report_date,
            SPCIngestionLog.success == True
        ).order_by(SPCIngestionLog.completed_at.desc()).first()
        
        if not last_log:
            return True  # Never polled before
            
        interval_minutes = self.get_polling_schedule(report_date)
        time_since_last = datetime.utcnow() - last_log.completed_at
        
        return time_since_last.total_seconds() >= (interval_minutes * 60)
    
    def format_date_for_url(self, report_date: date) -> str:
        """Convert date to YYMMDD format for SPC URL"""
        return report_date.strftime("%y%m%d")
    
    def poll_spc_reports(self, report_date: date = None) -> Dict:
        """
        Main method to poll SPC reports for a given date
        """
        if not report_date:
            report_date = date.today()
            
        # Check if we should poll based on schedule
        if not self.should_poll_now(report_date):
            return {
                'status': 'skipped',
                'message': f'Not time to poll {report_date} yet'
            }
        
        # Create ingestion log
        log = SPCIngestionLog()
        log.report_date = report_date
        log.started_at = datetime.utcnow()
        self.db.add(log)
        self.db.flush()  # Get the ID
        
        try:
            url = f"{self.base_url}{self.format_date_for_url(report_date)}_rpts_filtered.csv"
            log.url_attempted = url
            
            logger.info(f"Polling SPC reports from {url}")
            
            # Download CSV
            response = requests.get(url, timeout=Config.REQUEST_TIMEOUT)
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            logger.info(f"Response encoding: {response.encoding}")
            response.raise_for_status()
            
            # Check response content
            logger.info(f"Raw response length: {len(response.content)} bytes")
            logger.info(f"Text response length: {len(response.text)} characters")
            
            if not response.content:
                raise Exception("Empty response content received from SPC")
            
            if not response.text.strip():
                raise Exception("Empty text content after decoding")
            
            logger.info(f"First 500 chars: {repr(response.text[:500])}")
            
            # Sanitize CSV content - remove null characters that cause PostgreSQL errors
            clean_content = response.text.replace('\x00', '')
            if len(clean_content) != len(response.text):
                logger.warning(f"Removed {len(response.text) - len(clean_content)} null characters from CSV")
            
            # Parse CSV content
            result = self._parse_spc_csv(clean_content, report_date)
            
            # Check if we have more reports than before
            existing_count = SPCReport.query.filter(
                SPCReport.report_date == report_date
            ).count()
            
            logger.info(f"Found {result['total_reports']} reports in CSV, database has {existing_count}")
            
            if result['total_reports'] == 0:
                logger.warning(f"No reports parsed from CSV for {report_date}")
                log.success = True
                log.completed_at = datetime.utcnow()
                log.total_reports = 0
                self.db.commit()
                return {
                    'status': 'no_data_in_csv',
                    'existing_count': existing_count,
                    'message': f'No reports found in CSV for {report_date}'
                }
            
            # Store new reports
            stored_counts = self._store_reports(result['reports'], report_date)
            
            # Update log
            log.success = True
            log.completed_at = datetime.utcnow()
            log.tornado_reports = stored_counts['tornado']
            log.wind_reports = stored_counts['wind'] 
            log.hail_reports = stored_counts['hail']
            log.total_reports = sum(stored_counts.values())
            
            self.db.commit()
            
            logger.info(f"Successfully ingested {log.total_reports} SPC reports for {report_date}")
            
            return {
                'status': 'success',
                'date': report_date.isoformat(),
                'tornado_reports': stored_counts['tornado'],
                'wind_reports': stored_counts['wind'],
                'hail_reports': stored_counts['hail'],
                'total_reports': log.total_reports
            }
            
        except Exception as e:
            logger.error(f"Error ingesting SPC reports for {report_date}: {e}")
            log.error_message = str(e)
            log.completed_at = datetime.utcnow()
            self.db.commit()
            raise
    
    def _parse_spc_csv(self, csv_content: str, report_date: date) -> Dict:
        """
        Parse the multi-section SPC CSV content
        Returns dict with parsed reports by type
        """
        lines = csv_content.strip().split('\n')
        reports = []
        current_section = None
        current_headers = None
        
        tornado_count = wind_count = hail_count = 0
        
        for line_num, line in enumerate(lines):
            if not line.strip():
                continue
                
            # Detect section headers
            if self._is_header_line(line):
                current_section, current_headers = self._parse_header_line(line)
                logger.debug(f"Found {current_section} section at line {line_num + 1}")
                continue
            
            # Parse data lines
            if current_section and current_headers:
                try:
                    report = self._parse_report_line(
                        line, current_section, current_headers, report_date, line_num + 1
                    )
                    if report:
                        reports.append(report)
                        if current_section == 'tornado':
                            tornado_count += 1
                        elif current_section == 'wind':
                            wind_count += 1
                        elif current_section == 'hail':
                            hail_count += 1
                            
                except Exception as e:
                    logger.warning(f"Error parsing line {line_num + 1}: {line[:50]}... - {e}")
                    continue
        
        logger.info(f"CSV parsing complete: {len(lines)} total lines processed")
        logger.info(f"Sections detected: tornado={tornado_count}, wind={wind_count}, hail={hail_count}")
        logger.info(f"Total reports parsed: {len(reports)}")
        
        return {
            'reports': reports,
            'total_reports': len(reports),
            'tornado_count': tornado_count,
            'wind_count': wind_count,
            'hail_count': hail_count
        }
    
    def _is_header_line(self, line: str) -> bool:
        """Check if line is a section header"""
        # Look for Time header with section-specific columns
        if not line.startswith('Time,'):
            return False
        return any(indicator in line for indicator in ['F_Scale', 'Speed', 'Size'])
    
    def _parse_header_line(self, line: str) -> Tuple[str, List[str]]:
        """Parse header line to determine section type and column names"""
        headers = [h.strip() for h in line.split(',')]
        
        if 'F_Scale' in line:
            return 'tornado', headers
        elif 'Speed' in line:
            return 'wind', headers
        elif 'Size' in line:
            return 'hail', headers
        else:
            return 'unknown', headers
    
    def _parse_report_line(self, line: str, section_type: str, headers: List[str], 
                          report_date: date, line_num: int) -> Optional[Dict]:
        """Parse a single report line based on section type"""
        try:
            # Use CSV reader to handle quoted fields properly
            reader = csv.reader(StringIO(line))
            values = next(reader)
            
            if len(values) != len(headers):
                logger.warning(f"Column count mismatch at line {line_num}: expected {len(headers)}, got {len(values)}")
                return None
            
            # Create column mapping
            data = dict(zip(headers, values))
            
            # Extract common fields
            report = {
                'report_date': report_date,
                'report_type': section_type,
                'time_utc': data.get('Time', '').strip(),
                'location': data.get('Location', '').strip(),
                'county': data.get('County', '').strip(),
                'state': data.get('State', '').strip(),
                'comments': data.get('Comments', '').strip(),
                'raw_csv_line': line.strip()
            }
            
            # Parse coordinates
            try:
                report['latitude'] = float(data.get('Lat', 0)) if data.get('Lat') else None
                report['longitude'] = float(data.get('Lon', 0)) if data.get('Lon') else None
            except (ValueError, TypeError):
                report['latitude'] = None
                report['longitude'] = None
            
            # Parse magnitude based on section type
            if section_type == 'tornado':
                magnitude = data.get('F_Scale', '').strip()
                report['magnitude'] = {'f_scale': magnitude} if magnitude else {}
            elif section_type == 'wind':
                speed_raw = data.get('Speed', '').strip()
                if speed_raw == 'UNK':
                    report['magnitude'] = {'speed': 'UNK'}
                elif speed_raw:
                    try:
                        speed = int(speed_raw)
                        report['magnitude'] = {'speed': speed}
                    except (ValueError, TypeError):
                        report['magnitude'] = {}
                else:
                    report['magnitude'] = {}
            elif section_type == 'hail':
                size_raw = data.get('Size', '').strip()
                if size_raw == 'UNK':
                    report['magnitude'] = {'size': 'UNK'}
                elif size_raw:
                    try:
                        size_hundredths = int(size_raw)
                        size_inches = size_hundredths / 100.0
                        report['magnitude'] = {'size_hundredths': size_hundredths, 'size_inches': size_inches}
                    except (ValueError, TypeError):
                        report['magnitude'] = {}
                else:
                    report['magnitude'] = {}
            
            return report
            
        except Exception as e:
            logger.error(f"Error parsing report line {line_num}: {e}")
            return None
    
    def _store_reports(self, reports: List[Dict], report_date: date) -> Dict[str, int]:
        """Store parsed reports in database"""
        counts = {'tornado': 0, 'wind': 0, 'hail': 0}
        
        # Track processed unique keys to avoid duplicates within the same batch
        processed_keys = set()
        
        for report_data in reports:
            try:
                # Create unique key for duplicate detection using the full raw CSV line
                # This ensures we only filter true duplicates, not legitimate multiple reports
                unique_key = (
                    report_data['report_date'],
                    report_data['report_type'],
                    report_data['raw_csv_line']
                )
                
                # Skip if we've already processed this exact report in this batch
                if unique_key in processed_keys:
                    logger.debug(f"Duplicate within batch ignored: {report_data['location']} {report_data['time_utc']}")
                    continue
                
                # Check if exact same report already exists in database using raw CSV line
                existing = SPCReport.query.filter(
                    SPCReport.report_date == report_data['report_date'],
                    SPCReport.report_type == report_data['report_type'],
                    SPCReport.raw_csv_line == report_data['raw_csv_line']
                ).first()
                
                if existing:
                    logger.debug(f"Duplicate in database ignored: {report_data['location']} {report_data['time_utc']}")
                    continue
                
                # Create SPCReport object
                report = SPCReport(
                    report_date=report_data['report_date'],
                    report_type=report_data['report_type'],
                    time_utc=report_data['time_utc'],
                    location=report_data['location'],
                    county=report_data['county'],
                    state=report_data['state'],
                    latitude=report_data['latitude'],
                    longitude=report_data['longitude'],
                    comments=report_data['comments'],
                    magnitude=report_data['magnitude'],
                    raw_csv_line=report_data['raw_csv_line']
                )
                
                self.db.add(report)
                processed_keys.add(unique_key)
                counts[report_data['report_type']] += 1
                
            except Exception as e:
                logger.error(f"Error storing report: {e}")
                continue
        
        try:
            self.db.commit()
            logger.info(f"Successfully stored {sum(counts.values())} reports for {report_date}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error committing reports: {e}")
            raise
            
        return counts
    
    def get_ingestion_stats(self) -> Dict:
        """Get SPC ingestion statistics"""
        total_reports = SPCReport.query.count()
        
        # Reports by type
        type_counts = self.db.query(
            SPCReport.report_type,
            func.count(SPCReport.id).label('count')
        ).group_by(SPCReport.report_type).all()
        
        # Recent ingestion logs
        recent_logs = SPCIngestionLog.query.order_by(
            SPCIngestionLog.started_at.desc()
        ).limit(10).all()
        
        return {
            'total_reports': total_reports,
            'reports_by_type': {row.report_type: row.count for row in type_counts},
            'recent_ingestions': [
                {
                    'date': log.report_date.isoformat(),
                    'success': log.success,
                    'total_reports': log.total_reports,
                    'started_at': log.started_at.isoformat() if log.started_at else None,
                    'error': log.error_message
                }
                for log in recent_logs
            ]
        }