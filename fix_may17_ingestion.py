#!/usr/bin/env python3
"""
Comprehensive fix for May 17, 2024 data integrity issue
Manual insertion of all 102 reports with proper error handling
"""

import requests
from datetime import date
from app import app, db
from models import SPCReport
from sqlalchemy.exc import IntegrityError

def manual_fix_may17():
    """Manually ingest all 102 reports for May 17, 2024"""
    url = "https://www.spc.noaa.gov/climo/reports/240517_rpts_filtered.csv"
    response = requests.get(url)
    content = response.text
    lines = content.strip().split('\n')
    
    print(f"Processing {len(lines)} CSV lines for May 17, 2024")
    
    reports_to_insert = []
    current_section = None
    
    # Parse all sections
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
            
        # Detect section headers
        if line.startswith('Time,F_Scale'):
            current_section = 'tornado'
            print(f"Tornado section found at line {i+1}")
            continue
        elif line.startswith('Time,Speed'):
            current_section = 'wind'
            print(f"Wind section found at line {i+1}")
            continue
        elif line.startswith('Time,Size'):
            current_section = 'hail'
            print(f"Hail section found at line {i+1}")
            continue
        
        # Skip other header lines
        if line.startswith('Time,'):
            continue
            
        # Parse data lines
        if current_section and ',' in line:
            parts = line.split(',')
            if len(parts) >= 7:
                try:
                    time_utc = parts[0].strip()
                    magnitude_field = parts[1].strip()
                    location = parts[2].strip()
                    county = parts[3].strip()
                    state = parts[4].strip()
                    
                    # Parse coordinates
                    try:
                        latitude = float(parts[5]) if parts[5].strip() else None
                        longitude = float(parts[6]) if parts[6].strip() else None
                    except ValueError:
                        latitude = None
                        longitude = None
                    
                    # Extract comments
                    comments = ','.join(parts[7:]) if len(parts) > 7 else ''
                    
                    # Parse magnitude based on section
                    magnitude = {}
                    if current_section == 'tornado':
                        magnitude = {'f_scale': magnitude_field} if magnitude_field != 'UNK' else {}
                    elif current_section == 'wind':
                        if magnitude_field != 'UNK':
                            try:
                                magnitude = {'speed': int(magnitude_field)}
                            except ValueError:
                                magnitude = {'speed_text': magnitude_field}
                        else:
                            magnitude = {'speed_text': 'UNK'}
                    elif current_section == 'hail':
                        if magnitude_field != 'UNK':
                            try:
                                size = int(magnitude_field)
                                magnitude = {'size_hundredths': size, 'size_inches': size / 100.0}
                            except ValueError:
                                magnitude = {}
                    
                    report_data = {
                        'report_type': current_section,
                        'time_utc': time_utc,
                        'location': location,
                        'county': county,
                        'state': state,
                        'latitude': latitude,
                        'longitude': longitude,
                        'comments': comments,
                        'magnitude': magnitude,
                        'raw_csv_line': line
                    }
                    
                    reports_to_insert.append(report_data)
                    
                except Exception as e:
                    print(f"Error parsing line {i+1}: {line[:50]} - {e}")
    
    print(f"Parsed {len(reports_to_insert)} reports for insertion")
    
    # Group by type for verification
    by_type = {}
    for report in reports_to_insert:
        report_type = report['report_type']
        if report_type not in by_type:
            by_type[report_type] = 0
        by_type[report_type] += 1
    
    print(f"Report breakdown: {by_type}")
    
    # Insert into database with comprehensive error handling
    with app.app_context():
        success_count = 0
        duplicate_count = 0
        error_count = 0
        
        for i, report_data in enumerate(reports_to_insert):
            try:
                # Create SPCReport object
                report = SPCReport()
                report.report_date = date(2024, 5, 17)
                report.report_type = report_data['report_type']
                report.time_utc = report_data['time_utc']
                report.location = report_data['location']
                report.county = report_data['county']
                report.state = report_data['state']
                report.latitude = report_data['latitude']
                report.longitude = report_data['longitude']
                report.comments = report_data['comments']
                report.magnitude = report_data['magnitude']
                report.raw_csv_line = report_data['raw_csv_line']
                
                # Insert individual record
                db.session.add(report)
                db.session.commit()
                
                success_count += 1
                print(f"✓ {i+1:3d}: {report_data['report_type']} - {report_data['location']}")
                
            except IntegrityError as ie:
                db.session.rollback()
                duplicate_count += 1
                if 'uq_spc_report_csv_unique' in str(ie):
                    print(f"✗ {i+1:3d}: DUPLICATE - {report_data['location']}")
                else:
                    print(f"✗ {i+1:3d}: CONSTRAINT - {report_data['location']} - {ie}")
                    
            except Exception as e:
                db.session.rollback()
                error_count += 1
                print(f"✗ {i+1:3d}: ERROR - {report_data['location']} - {e}")
        
        # Final verification
        final_count = db.session.query(SPCReport).filter(SPCReport.report_date == date(2024, 5, 17)).count()
        tornado_count = db.session.query(SPCReport).filter(
            SPCReport.report_date == date(2024, 5, 17),
            SPCReport.report_type == 'tornado'
        ).count()
        wind_count = db.session.query(SPCReport).filter(
            SPCReport.report_date == date(2024, 5, 17),
            SPCReport.report_type == 'wind'
        ).count()
        hail_count = db.session.query(SPCReport).filter(
            SPCReport.report_date == date(2024, 5, 17),
            SPCReport.report_type == 'hail'
        ).count()
        
        print(f"\nFinal Results:")
        print(f"Successfully inserted: {success_count}")
        print(f"Duplicates skipped: {duplicate_count}")
        print(f"Errors encountered: {error_count}")
        print(f"Total in database: {final_count}")
        print(f"Breakdown - Tornado: {tornado_count}, Wind: {wind_count}, Hail: {hail_count}")
        
        return {
            'success': success_count,
            'duplicates': duplicate_count,
            'errors': error_count,
            'total_stored': final_count,
            'tornado': tornado_count,
            'wind': wind_count,
            'hail': hail_count
        }

if __name__ == "__main__":
    result = manual_fix_may17()
    print(f"\nOperation completed: {result}")