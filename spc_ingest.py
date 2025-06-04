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
            
            # Download CSV with proper headers to get complete data
            headers = {
                'User-Agent': 'HailyDB-SPC-Ingestion/2.0 (contact@hailydb.com)',
                'Accept': 'text/csv,text/plain,*/*',
                'Accept-Encoding': 'identity',  # Disable compression to avoid truncation
                'Connection': 'keep-alive'
            }
            response = requests.get(url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
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
        Parse multi-section SPC CSV with comprehensive malformation handling
        Returns dict with parsed reports by type
        """
        # Pre-process CSV to handle multi-line records and truncated lines
        processed_content = self._preprocess_csv_content(csv_content)
        lines = processed_content.strip().split('\n')
        
        reports = []
        current_section = None
        current_headers = None
        
        tornado_count = wind_count = hail_count = 0
        failed_lines = []
        
        for line_num, line in enumerate(lines):
            if not line.strip():
                continue
                
            # Detect section headers
            if self._is_header_line(line):
                current_section, current_headers = self._parse_header_line(line)
                logger.debug(f"Found {current_section} section at line {line_num + 1}")
                continue
            
            # Parse data lines with comprehensive error recovery
            if current_section and current_headers:
                report = None
                
                # Primary parser attempt
                try:
                    report = self._parse_report_line(
                        line, current_section, current_headers, report_date, line_num + 1
                    )
                except Exception as e:
                    logger.debug(f"Primary parser failed line {line_num + 1}: {e}")
                
                # Emergency fallback if primary fails
                if not report:
                    try:
                        report = self._emergency_parse_line(
                            line, current_section, report_date, line_num + 1
                        )
                        if report:
                            logger.info(f"Emergency parser recovered line {line_num + 1}")
                    except Exception as e:
                        logger.debug(f"Emergency parser failed line {line_num + 1}: {e}")
                
                # Final aggressive recovery attempt
                if not report:
                    report = self._aggressive_recovery_parse(
                        line, current_section, report_date, line_num + 1
                    )
                    if report:
                        logger.info(f"Aggressive recovery succeeded line {line_num + 1}")
                
                # Store results
                if report:
                    reports.append(report)
                    if current_section == 'tornado':
                        tornado_count += 1
                    elif current_section == 'wind':
                        wind_count += 1
                    elif current_section == 'hail':
                        hail_count += 1
                else:
                    failed_lines.append((line_num + 1, line[:100]))
                    logger.error(f"Complete parsing failure at line {line_num + 1}: {line[:100]}")
        
        logger.info(f"CSV parsing complete: {len(lines)} total lines processed")
        logger.info(f"Sections detected: tornado={tornado_count}, wind={wind_count}, hail={hail_count}")
        logger.info(f"Total reports parsed: {len(reports)}")
        logger.info(f"Failed to parse {len(failed_lines)} lines")
        
        return {
            'reports': reports,
            'total_reports': len(reports),
            'tornado_count': tornado_count,
            'wind_count': wind_count,
            'hail_count': hail_count,
            'failed_lines': failed_lines
        }
    
    def _preprocess_csv_content(self, csv_content: str) -> str:
        """Pre-process CSV to handle multi-line records and formatting issues"""
        lines = csv_content.split('\n')
        processed_lines = []
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Skip empty lines
            if not line:
                i += 1
                continue
            
            # Keep header lines as-is
            if self._is_header_line(line):
                processed_lines.append(line)
                i += 1
                continue
            
            # For data lines, check if they're complete (have minimum field count)
            if line.count(',') >= 6:  # Minimum fields for valid SPC record
                processed_lines.append(line)
                i += 1
            else:
                # Potentially incomplete line - try to merge with next line
                merged_line = line
                j = i + 1
                while j < len(lines) and merged_line.count(',') < 6:
                    next_line = lines[j].strip()
                    if next_line and not self._is_header_line(next_line):
                        merged_line += " " + next_line
                        j += 1
                    else:
                        break
                
                processed_lines.append(merged_line)
                i = j
        
        return '\n'.join(processed_lines)
    
    def _aggressive_recovery_parse(self, line: str, section_type: str, report_date: date, line_num: int) -> Optional[Dict]:
        """Final attempt parser using minimal field extraction"""
        try:
            line = line.strip()
            if not line or len(line) < 10:
                return None
            
            # Extract any recognizable patterns with regex
            import re
            
            # Pattern to match time (4 digits), magnitude, and basic location info
            basic_pattern = r'^(\d{4}),([^,]*),([^,]+)'
            match = re.match(basic_pattern, line)
            
            if not match:
                return None
            
            time_val, mag_val, location_val = match.groups()
            
            # Extract additional fields using simple parsing
            remaining = line[match.end():]
            parts = remaining.split(',')
            
            # Build minimal viable record
            report = {
                'report_date': report_date,
                'report_type': section_type,
                'time_utc': time_val.strip(),
                'location': location_val.strip(),
                'county': parts[0].strip() if len(parts) > 0 else 'Unknown',
                'state': parts[1].strip() if len(parts) > 1 else 'UNK',
                'latitude': None,
                'longitude': None,
                'comments': ','.join(parts[4:]).strip() if len(parts) > 4 else '',
                'magnitude': self._parse_magnitude(mag_val.strip(), section_type),
                'raw_csv_line': line
            }
            
            # Try to extract coordinates
            for part in parts:
                try:
                    val = float(part.strip())
                    if 20 <= abs(val) <= 90 and report['latitude'] is None:
                        report['latitude'] = val
                    elif 60 <= abs(val) <= 180 and report['longitude'] is None:
                        report['longitude'] = val
                except ValueError:
                    continue
            
            return report
            
        except Exception as e:
            logger.debug(f"Aggressive recovery failed line {line_num}: {e}")
            return None
    
    def _parse_magnitude(self, mag_str: str, section_type: str) -> dict:
        """Parse magnitude field based on section type"""
        try:
            if section_type == 'tornado':
                return {'f_scale': mag_str} if mag_str != 'UNK' else {}
            elif section_type == 'wind':
                if mag_str == 'UNK':
                    return {'speed_text': 'UNK', 'speed': None}
                try:
                    speed = int(mag_str)
                    return {'speed': speed}
                except ValueError:
                    return {'speed_text': mag_str, 'speed': None}
            elif section_type == 'hail':
                try:
                    size = int(mag_str)
                    return {'size_hundredths': size, 'size_inches': size / 100.0}
                except ValueError:
                    return {}
            return {}
        except Exception:
            return {}
    
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
    
    def _emergency_parse_line(self, line: str, section_type: str, report_date: date, line_num: int) -> Optional[Dict]:
        """
        Emergency fallback parser for critically malformed SPC CSV lines
        Uses aggressive pattern matching to extract essential data
        """
        try:
            line = line.strip()
            if not line:
                return None
            
            # Split by comma and extract what we can
            parts = [p.strip() for p in line.split(',')]
            
            if len(parts) < 4:  # Absolute minimum: Time, Mag, Location, County
                return None
            
            # Emergency field extraction with defaults
            time_field = parts[0] if parts[0] else "0000"
            magnitude_field = parts[1] if len(parts) > 1 else "UNK"
            location_field = parts[2] if len(parts) > 2 else "Unknown"
            
            # Extract county, state, coordinates with fallbacks
            county_field = "Unknown"
            state_field = "UNK"
            latitude = None
            longitude = None
            comments = ""
            
            # Scan for recognizable patterns
            for i, part in enumerate(parts):
                # Look for state codes (2-letter uppercase)
                if len(part) == 2 and part.isalpha() and part.isupper():
                    state_field = part
                    if i > 0:
                        county_field = parts[i-1]
                
                # Look for coordinates (numeric with decimal)
                try:
                    float_val = float(part)
                    if -180 <= float_val <= 180:
                        if latitude is None and 20 <= abs(float_val) <= 90:
                            latitude = float_val
                        elif longitude is None and 60 <= abs(float_val) <= 180:
                            longitude = float_val
                except ValueError:
                    continue
            
            # Join remaining parts as comments
            if len(parts) > 7:
                comments = ','.join(parts[7:])
            
            # Create magnitude structure based on section type
            if section_type == 'tornado':
                magnitude = {'f_scale': magnitude_field} if magnitude_field != 'UNK' else {}
            elif section_type == 'wind':
                if magnitude_field == 'UNK':
                    magnitude = {'speed_text': 'UNK', 'speed': None}
                else:
                    try:
                        speed = int(magnitude_field)
                        magnitude = {'speed': speed}
                    except ValueError:
                        magnitude = {'speed_text': magnitude_field, 'speed': None}
            elif section_type == 'hail':
                try:
                    size = int(magnitude_field)
                    magnitude = {
                        'size_hundredths': size,
                        'size_inches': size / 100.0
                    }
                except ValueError:
                    magnitude = {}
            else:
                magnitude = {}
            
            # Build report structure
            report = {
                'report_date': report_date,
                'report_type': section_type,
                'time_utc': time_field,
                'location': location_field,
                'county': county_field,
                'state': state_field,
                'latitude': latitude,
                'longitude': longitude,
                'comments': comments,
                'magnitude': magnitude,
                'raw_csv_line': line
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Emergency parser failed on line {line_num}: {e}")
            return None
    
    def _parse_report_line(self, line: str, section_type: str, headers: List[str], 
                          report_date: date, line_num: int) -> Optional[Dict]:
        """Bulletproof SPC CSV parser with comprehensive error recovery"""
        try:
            line = line.strip()
            if not line:
                return None
            
            # COMPREHENSIVE SPC CSV PARSING STRATEGY
            # SPC CSV has multiple malformation patterns that must be handled systematically
            
            # Step 1: Handle the most common pattern - unquoted commas in comments field
            # Strategy: Find first 7 comma positions, everything after is comments
            comma_positions = []
            for i, char in enumerate(line):
                if char == ',':
                    comma_positions.append(i)
                if len(comma_positions) >= 7:
                    break
            
            # Extract fields based on comma positions
            if len(comma_positions) >= 6:  # Need at least 6 commas for 7 fields + comments
                values = []
                start = 0
                
                # Extract first 7 fields
                for pos in comma_positions[:6]:
                    values.append(line[start:pos].strip())
                    start = pos + 1
                
                # Extract 7th field (before 7th comma if it exists)
                if len(comma_positions) >= 7:
                    values.append(line[start:comma_positions[6]].strip())
                    # Everything after 7th comma is comments
                    values.append(line[comma_positions[6]+1:].strip())
                else:
                    # No 7th comma - rest is the 7th field, empty comments
                    values.append(line[start:].strip())
                    values.append('')
            else:
                # Fallback for lines with insufficient commas
                parts = line.split(',')
                if len(parts) < 7:
                    logger.warning(f"Insufficient fields at line {line_num}: {len(parts)} < 7")
                    return None
                
                values = parts[:7]
                if len(parts) > 7:
                    values.append(','.join(parts[7:]))
                else:
                    values.append('')
            
            # Step 2: Handle extra state field malformation
            # Pattern: Time,Mag,Location,ExtraState,County,State,Lat,Lon,Comments
            # Detect by checking if field 3 is a 2-letter state code
            if (len(values) >= 8 and len(line.split(',')) >= 9 and 
                len(values[3]) == 2 and values[3].isalpha() and values[3].isupper()):
                
                # Merge location with extra state
                parts = line.split(',')
                values = [
                    parts[0].strip(),  # Time
                    parts[1].strip(),  # Magnitude
                    f"{parts[2]} {parts[3]}".strip(),  # Location + ExtraState
                    parts[4].strip(),  # County
                    parts[5].strip(),  # State
                    parts[6].strip(),  # Lat
                    parts[7].strip(),  # Lon
                    ','.join(parts[8:]).strip() if len(parts) > 8 else ''  # Comments
                ]
                logger.debug(f"Merged extra state field at line {line_num}")
            
            # Step 3: Validate and normalize
            values = [str(v).strip() for v in values]
            
            # Ensure exactly the right number of fields
            while len(values) < len(headers):
                values.append('')
            values = values[:len(headers)]
            
            if len(values) != len(headers):
                logger.error(f"Field count mismatch at line {line_num}: {len(values)} vs {len(headers)}")
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
                    # Store UNK as string field to avoid PostgreSQL JSONB validation errors
                    report['magnitude'] = {'speed_text': 'UNK', 'speed': None}
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
                    # Store UNK as string field to avoid PostgreSQL JSONB validation errors
                    report['magnitude'] = {'size_text': 'UNK', 'size': None}
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
    
    def _store_reports(self, reports: List[Dict], report_date: date, is_reimport: bool = False) -> Dict[str, int]:
        """Store parsed reports in database with batch processing to avoid timeouts"""
        counts = {'tornado': 0, 'wind': 0, 'hail': 0}
        processed_keys = set() if not is_reimport else None  # Skip duplicate tracking for reimports
        batch_size = 50  # Process in smaller batches
        
        # Process reports in batches to avoid timeout with large datasets
        for i in range(0, len(reports), batch_size):
            batch = reports[i:i + batch_size]
            batch_count = 0
            
            for report_data in batch:
                try:
                    # Check duplicates only for regular operations, not for direct SPC ingestion or reimports
                    if not is_reimport and processed_keys is not None:
                        # Use raw CSV line as unique key - this is the complete row content
                        raw_line = report_data['raw_csv_line']
                        
                        # Skip if this exact raw CSV line was already processed in this session
                        if raw_line in processed_keys:
                            continue
                    
                    # Create SPCReport object with proper JSON handling
                    try:
                        # Ensure magnitude is proper dict, not stringified JSON
                        magnitude_data = report_data['magnitude']
                        if isinstance(magnitude_data, str):
                            import json
                            try:
                                magnitude_data = json.loads(magnitude_data)
                            except (json.JSONDecodeError, TypeError):
                                magnitude_data = {}
                        
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
                            magnitude=magnitude_data,
                            raw_csv_line=report_data['raw_csv_line']
                        )
                        
                        self.db.add(report)
                        
                    except Exception as insert_error:
                        logger.error(f"Failed to insert report at line {report_data.get('raw_csv_line', 'unknown')[:100]}: {insert_error}")
                        # Skip this report and continue with the next one
                        continue
                    
                    # Only track processed keys for regular operations
                    if not is_reimport and processed_keys is not None:
                        processed_keys.add(raw_line)
                    
                    counts[report_data['report_type']] += 1
                    batch_count += 1
                    
                except Exception as e:
                    logger.error(f"Error storing report: {e}")
                    continue
            
            # Flush batch to database but don't commit yet for reimports
            try:
                if batch_count > 0:
                    self.db.flush()
                    if not is_reimport:
                        # Only commit immediately for regular operations
                        self.db.commit()
                    logger.info(f"Batch {i//batch_size + 1}: stored {batch_count} reports")
            except Exception as e:
                self.db.rollback()
                logger.error(f"Error processing batch {i//batch_size + 1}: {e}")
                # Reset counts for failed batch
                for report_data in batch:
                    if report_data['report_type'] in counts:
                        counts[report_data['report_type']] = max(0, counts[report_data['report_type']] - 1)
                continue
        
        logger.info(f"Successfully stored {sum(counts.values())} total reports for {report_date}")
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

    def reimport_spc_reports(self, report_date: date) -> Dict:
        """
        Reimport SPC reports for a specific date (bypass duplicate detection)
        Used by the reimport endpoint to ensure complete data replacement
        """
        # Create ingestion log
        log = SPCIngestionLog()
        log.report_date = report_date
        log.started_at = datetime.utcnow()
        self.db.add(log)
        self.db.flush()
        
        try:
            url = f"{self.base_url}{self.format_date_for_url(report_date)}_rpts_filtered.csv"
            log.url_attempted = url
            
            logger.info(f"Reimporting SPC reports from {url}")
            
            # Download CSV with proper headers
            headers = {
                'User-Agent': 'HailyDB-SPC-Ingestion/2.0 (contact@hailydb.com)',
                'Accept': 'text/csv,text/plain,*/*',
                'Accept-Encoding': 'identity',
                'Connection': 'keep-alive'
            }
            response = requests.get(url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()
            
            # Sanitize CSV content
            clean_content = response.text.replace('\x00', '')
            
            # Parse CSV content
            result = self._parse_spc_csv(clean_content, report_date)
            
            if result['total_reports'] == 0:
                log.success = True
                log.completed_at = datetime.utcnow()
                log.total_reports = 0
                self.db.commit()
                return {
                    'status': 'no_data_in_csv',
                    'message': f'No reports found in CSV for {report_date}'
                }
            
            # Store reports with reimport flag to bypass duplicate detection
            stored_counts = self._store_reports(result['reports'], report_date, is_reimport=True)
            
            # Update log
            log.success = True
            log.completed_at = datetime.utcnow()
            log.tornado_reports = stored_counts['tornado']
            log.wind_reports = stored_counts['wind'] 
            log.hail_reports = stored_counts['hail']
            log.total_reports = sum(stored_counts.values())
            
            self.db.commit()
            
            logger.info(f"Successfully reimported {log.total_reports} SPC reports for {report_date}")
            
            return {
                'status': 'success',
                'reports_ingested': log.total_reports,
                'tornado': log.tornado_reports,
                'wind': log.wind_reports,
                'hail': log.hail_reports
            }
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error reimporting SPC reports for {report_date}: {e}")
            
            log.error_message = str(e)
            log.completed_at = datetime.utcnow()
            self.db.commit()
            
            return {
                'status': 'error',
                'error': str(e)
            }