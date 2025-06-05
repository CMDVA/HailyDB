#!/usr/bin/env python3
"""
Debug script to identify data loss during May 17, 2024 SPC ingestion
Compares raw CSV data with database records to find missing reports
"""

import requests
import csv
from io import StringIO
from datetime import date
from app import app, db
from models import SPCReport
from sqlalchemy import func

def download_and_parse_csv():
    """Download and parse the raw CSV file"""
    url = "https://www.spc.noaa.gov/climo/reports/240517_rpts_filtered.csv"
    response = requests.get(url)
    response.raise_for_status()
    
    content = response.text
    lines = content.strip().split('\n')
    
    print(f"Total CSV lines: {len(lines)}")
    
    # Parse by sections
    reports = []
    current_section = None
    
    for i, line in enumerate(lines):
        if line.startswith('Time,F_Scale'):
            current_section = 'tornado'
            print(f"Tornado section starts at line {i+1}")
            continue
        elif line.startswith('Time,Speed'):
            current_section = 'wind'
            print(f"Wind section starts at line {i+1}")
            continue
        elif line.startswith('Time,Size'):
            current_section = 'hail'
            print(f"Hail section starts at line {i+1}")
            continue
        
        # Skip empty lines and headers
        if not line.strip() or line.startswith('Time,'):
            continue
            
        # Parse data line
        if current_section and ',' in line:
            try:
                parts = line.split(',')
                if len(parts) >= 7:
                    time_utc = parts[0].strip()
                    location = parts[2].strip()
                    county = parts[3].strip()
                    state = parts[4].strip()
                    
                    # Create unique identifier for this report
                    report_id = f"{time_utc}_{location}_{county}_{state}_{current_section}"
                    
                    reports.append({
                        'report_type': current_section,
                        'time_utc': time_utc,
                        'location': location,
                        'county': county,
                        'state': state,
                        'raw_line': line,
                        'unique_id': report_id
                    })
            except Exception as e:
                print(f"Failed to parse line {i+1}: {line[:50]} - {e}")
    
    print(f"Parsed {len(reports)} reports from CSV")
    return reports

def get_database_reports():
    """Get all reports from database for May 17, 2024"""
    with app.app_context():
        db_reports = db.session.query(SPCReport).filter(
            SPCReport.report_date == date(2024, 5, 17)
        ).all()
        
        reports = []
        for report in db_reports:
            report_id = f"{report.time_utc}_{report.location}_{report.county}_{report.state}_{report.report_type}"
            reports.append({
                'report_type': report.report_type,
                'time_utc': report.time_utc,
                'location': report.location,
                'county': report.county,
                'state': report.state,
                'raw_line': report.raw_csv_line,
                'unique_id': report_id
            })
        
        print(f"Found {len(reports)} reports in database")
        return reports

def compare_datasets():
    """Compare CSV and database reports to find missing data"""
    csv_reports = download_and_parse_csv()
    db_reports = get_database_reports()
    
    # Create sets of unique identifiers
    csv_ids = {report['unique_id'] for report in csv_reports}
    db_ids = {report['unique_id'] for report in db_reports}
    
    # Find differences
    missing_in_db = csv_ids - db_ids
    extra_in_db = db_ids - csv_ids
    
    print(f"\nComparison Results:")
    print(f"CSV reports: {len(csv_reports)}")
    print(f"Database reports: {len(db_reports)}")
    print(f"Missing in database: {len(missing_in_db)}")
    print(f"Extra in database: {len(extra_in_db)}")
    
    if missing_in_db:
        print(f"\nReports missing from database:")
        for report_id in missing_in_db:
            # Find the full report data
            for report in csv_reports:
                if report['unique_id'] == report_id:
                    print(f"  {report['report_type']}: {report['time_utc']} {report['location']} {report['county']}, {report['state']}")
                    print(f"    Raw: {report['raw_line']}")
                    break
    
    if extra_in_db:
        print(f"\nExtra reports in database:")
        for report_id in extra_in_db:
            # Find the full report data
            for report in db_reports:
                if report['unique_id'] == report_id:
                    print(f"  {report['report_type']}: {report['time_utc']} {report['location']} {report['county']}, {report['state']}")
                    break
    
    # Check for duplicates in CSV
    csv_raw_lines = [report['raw_line'] for report in csv_reports]
    unique_lines = set(csv_raw_lines)
    if len(csv_raw_lines) != len(unique_lines):
        print(f"\nDuplicate lines found in CSV:")
        seen = set()
        for line in csv_raw_lines:
            if line in seen:
                print(f"  DUPLICATE: {line}")
            else:
                seen.add(line)

if __name__ == "__main__":
    compare_datasets()