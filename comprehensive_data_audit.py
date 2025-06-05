#!/usr/bin/env python3
"""
Comprehensive Data Integrity Audit for SPC Reports
Identifies exact discrepancies between CSV source and database storage
"""

import requests
import csv
from io import StringIO
from datetime import datetime
from sqlalchemy import create_engine, text
import os
import json

def audit_date(date_str):
    """Perform comprehensive audit for a specific date"""
    print(f"\n=== COMPREHENSIVE AUDIT FOR {date_str} ===")
    
    # Convert date format for URL (YYYY-MM-DD -> YYMMDD)
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    url_date = date_obj.strftime('%y%m%d')
    url = f"https://www.spc.noaa.gov/climo/reports/{url_date}_rpts_filtered.csv"
    
    print(f"1. Downloading CSV from: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        csv_content = response.text
        
        print(f"2. CSV Response Status: {response.status_code}")
        print(f"3. CSV Content Length: {len(csv_content)} characters")
        
        # Parse CSV line by line
        lines = csv_content.strip().split('\n')
        print(f"4. Total CSV Lines: {len(lines)}")
        
        # Analyze CSV structure
        tornado_section_start = None
        wind_section_start = None
        hail_section_start = None
        
        for i, line in enumerate(lines):
            if line.startswith('Time,F_Scale'):
                tornado_section_start = i
                print(f"   Tornado section starts at line {i+1}")
            elif line.startswith('Time,Speed'):
                wind_section_start = i
                print(f"   Wind section starts at line {i+1}")
            elif line.startswith('Time,Size'):
                hail_section_start = i
                print(f"   Hail section starts at line {i+1}")
        
        # Count data lines in each section
        tornado_count = 0
        wind_count = 0
        hail_count = 0
        
        current_section = None
        for i, line in enumerate(lines):
            if line.startswith('Time,F_Scale'):
                current_section = 'tornado'
                continue
            elif line.startswith('Time,Speed'):
                current_section = 'wind'
                continue
            elif line.startswith('Time,Size'):
                current_section = 'hail'
                continue
            
            # Skip empty lines
            if not line.strip():
                continue
                
            # Count data lines
            if current_section == 'tornado' and not line.startswith('Time,'):
                tornado_count += 1
            elif current_section == 'wind' and not line.startswith('Time,'):
                wind_count += 1
            elif current_section == 'hail' and not line.startswith('Time,'):
                hail_count += 1
        
        print(f"5. CSV Data Counts:")
        print(f"   Tornado reports: {tornado_count}")
        print(f"   Wind reports: {wind_count}")
        print(f"   Hail reports: {hail_count}")
        print(f"   Total data reports: {tornado_count + wind_count + hail_count}")
        
        # Check database
        engine = create_engine(os.environ.get('DATABASE_URL'))
        
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_count,
                    SUM(CASE WHEN report_type = 'tornado' THEN 1 ELSE 0 END) as tornado_count,
                    SUM(CASE WHEN report_type = 'wind' THEN 1 ELSE 0 END) as wind_count,
                    SUM(CASE WHEN report_type = 'hail' THEN 1 ELSE 0 END) as hail_count
                FROM spc_reports 
                WHERE report_date = :date_str
            """), {"date_str": date_str})
            
            db_data = result.fetchone()
            
        print(f"6. Database Counts:")
        print(f"   Tornado reports: {db_data.tornado_count}")
        print(f"   Wind reports: {db_data.wind_count}")
        print(f"   Hail reports: {db_data.hail_count}")
        print(f"   Total database reports: {db_data.total_count}")
        
        # Calculate discrepancies
        tornado_diff = tornado_count - db_data.tornado_count
        wind_diff = wind_count - db_data.wind_count
        hail_diff = hail_count - db_data.hail_count
        total_diff = (tornado_count + wind_count + hail_count) - db_data.total_count
        
        print(f"7. DISCREPANCIES:")
        print(f"   Tornado missing: {tornado_diff}")
        print(f"   Wind missing: {wind_diff}")
        print(f"   Hail missing: {hail_diff}")
        print(f"   Total missing: {total_diff}")
        
        if total_diff > 0:
            print(f"   ❌ DATA LOSS DETECTED: {total_diff} reports missing from database")
            
            # Analyze specific missing lines
            print(f"\n8. ANALYZING MISSING DATA...")
            
            # Parse CSV again and check each line
            current_section = None
            failed_lines = []
            
            for i, line in enumerate(lines):
                if line.startswith('Time,F_Scale'):
                    current_section = 'tornado'
                    continue
                elif line.startswith('Time,Speed'):
                    current_section = 'wind'
                    continue
                elif line.startswith('Time,Size'):
                    current_section = 'hail'
                    continue
                
                if not line.strip() or line.startswith('Time,'):
                    continue
                
                # Try to parse this line
                try:
                    parts = line.split(',')
                    if len(parts) < 7:
                        failed_lines.append(f"Line {i+1}: Insufficient columns ({len(parts)}) - {line[:100]}")
                    elif not parts[0].strip():  # Empty time
                        failed_lines.append(f"Line {i+1}: Empty time field - {line[:100]}")
                    elif not parts[1].strip():  # Empty magnitude
                        failed_lines.append(f"Line {i+1}: Empty magnitude field - {line[:100]}")
                except Exception as e:
                    failed_lines.append(f"Line {i+1}: Parse error {e} - {line[:100]}")
            
            if failed_lines:
                print(f"   Found {len(failed_lines)} problematic lines:")
                for failed in failed_lines[:10]:  # Show first 10
                    print(f"   - {failed}")
                if len(failed_lines) > 10:
                    print(f"   ... and {len(failed_lines) - 10} more")
            else:
                print(f"   No obvious parsing issues found in CSV - problem may be in database insertion")
        else:
            print(f"   ✅ NO DATA LOSS: All CSV reports are in database")
        
        return {
            'date': date_str,
            'csv_counts': {
                'tornado': tornado_count,
                'wind': wind_count,
                'hail': hail_count,
                'total': tornado_count + wind_count + hail_count
            },
            'db_counts': {
                'tornado': int(db_data.tornado_count),
                'wind': int(db_data.wind_count),
                'hail': int(db_data.hail_count),
                'total': int(db_data.total_count)
            },
            'discrepancies': {
                'tornado': tornado_diff,
                'wind': wind_diff,
                'hail': hail_diff,
                'total': total_diff
            },
            'status': 'CLEAN' if total_diff == 0 else 'DATA_LOSS'
        }
        
    except Exception as e:
        print(f"ERROR: {e}")
        return {'date': date_str, 'error': str(e)}

def main():
    """Run audit on multiple dates"""
    dates_to_audit = [
        '2025-03-15',  # User reported issue
        '2024-05-17',  # Previously fixed
        '2025-04-15',  # Previously checked
    ]
    
    results = []
    
    for date_str in dates_to_audit:
        result = audit_date(date_str)
        results.append(result)
    
    print(f"\n=== SUMMARY REPORT ===")
    for result in results:
        if 'error' in result:
            print(f"{result['date']}: ERROR - {result['error']}")
        else:
            status = result['status']
            total_diff = result['discrepancies']['total']
            print(f"{result['date']}: {status} (missing: {total_diff})")
    
    # Save detailed results
    with open('data_audit_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nDetailed results saved to: data_audit_results.json")

if __name__ == "__main__":
    main()