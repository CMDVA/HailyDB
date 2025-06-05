#!/usr/bin/env python3
"""
Debug script to identify why tornado reports are failing database insertion
Tests each tornado report individually to find the exact failure point
"""

import requests
from datetime import date
from app import app, db
from models import SPCReport
from sqlalchemy.exc import IntegrityError

def test_tornado_insertion():
    """Test individual tornado report insertion"""
    url = "https://www.spc.noaa.gov/climo/reports/240517_rpts_filtered.csv"
    response = requests.get(url)
    content = response.text
    lines = content.strip().split('\n')
    
    tornado_reports = []
    
    # Extract tornado reports (lines 2-8)
    for i in range(2, 9):  # Lines 2-8 are tornado reports
        if i < len(lines):
            line = lines[i]
            parts = line.split(',')
            if len(parts) >= 7:
                tornado_reports.append({
                    'line_num': i,
                    'time_utc': parts[0],
                    'location': parts[2],
                    'county': parts[3],
                    'state': parts[4],
                    'latitude': float(parts[5]) if parts[5] else None,
                    'longitude': float(parts[6]) if parts[6] else None,
                    'comments': ','.join(parts[7:]) if len(parts) > 7 else '',
                    'raw_line': line
                })
    
    print(f"Found {len(tornado_reports)} tornado reports to test")
    
    with app.app_context():
        # Test each tornado report individually
        for i, report_data in enumerate(tornado_reports):
            try:
                # Create SPCReport object
                report = SPCReport()
                report.report_date = date(2024, 5, 17)
                report.report_type = 'tornado'
                report.time_utc = report_data['time_utc']
                report.location = report_data['location']
                report.county = report_data['county']
                report.state = report_data['state']
                report.latitude = report_data['latitude']
                report.longitude = report_data['longitude']
                report.comments = report_data['comments']
                report.magnitude = {'f_scale': 'UNK'}
                report.raw_csv_line = report_data['raw_line']
                
                # Add and flush individual record
                db.session.add(report)
                db.session.flush()
                db.session.commit()
                
                print(f"✓ SUCCESS: Tornado {i+1} - {report_data['location']}")
                
            except IntegrityError as ie:
                db.session.rollback()
                print(f"✗ INTEGRITY ERROR: Tornado {i+1} - {report_data['location']}")
                print(f"  Error: {ie}")
                
            except Exception as e:
                db.session.rollback()
                print(f"✗ GENERAL ERROR: Tornado {i+1} - {report_data['location']}")
                print(f"  Error: {e}")
                print(f"  Raw line: {report_data['raw_line']}")
        
        # Check final count
        final_count = db.session.query(SPCReport).filter(
            SPCReport.report_date == date(2024, 5, 17),
            SPCReport.report_type == 'tornado'
        ).count()
        print(f"\nFinal tornado count in database: {final_count}")

if __name__ == "__main__":
    test_tornado_insertion()