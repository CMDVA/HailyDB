#!/usr/bin/env python3
"""
Complete May 26, 2024 restoration - comprehensive fix to achieve 849/849 reports
Addresses the contradiction by ensuring all reports are properly restored
"""
import os
import sys
import requests
import json
import hashlib
from datetime import datetime, date
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Set up database connection
DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def parse_csv_line(line, section_type, line_num):
    """Parse any SPC CSV line based on section type"""
    try:
        fields = line.strip().split(',')
        if len(fields) < 7:
            return None
            
        time_str = fields[0]
        
        if section_type == 'tornado':
            f_scale = fields[1] if len(fields) > 1 else ""
            location = fields[2] if len(fields) > 2 else ""
            county = fields[3] if len(fields) > 3 else ""
            state = fields[4] if len(fields) > 4 else ""
            magnitude = {'f_scale': f_scale}
        elif section_type == 'wind':
            speed = fields[1] if len(fields) > 1 else ""
            location = fields[2] if len(fields) > 2 else ""
            county = fields[3] if len(fields) > 3 else ""
            state = fields[4] if len(fields) > 4 else ""
            magnitude = {'speed': speed}
        elif section_type == 'hail':
            size = fields[1] if len(fields) > 1 else ""
            location = fields[2] if len(fields) > 2 else ""
            county = fields[3] if len(fields) > 3 else ""
            state = fields[4] if len(fields) > 4 else ""
            magnitude = {'size': size}
        else:
            return None
        
        # Parse coordinates
        latitude = None
        longitude = None
        if len(fields) > 5 and fields[5]:
            try:
                latitude = float(fields[5])
            except:
                pass
        if len(fields) > 6 and fields[6]:
            try:
                longitude = float(fields[6])
            except:
                pass
        
        # Get comments
        comments = fields[7] if len(fields) > 7 else ""
        
        # Generate hash for duplicate detection
        hash_data = f"2024-05-26|{section_type}|{time_str}|{location}|{county}|{state}|{latitude or ''}|{longitude or ''}|{json.dumps(magnitude)}"
        clean_hash_data = hash_data.replace('\x00', '').replace('\r', '').replace('\n', ' ')
        row_hash = hashlib.sha256(clean_hash_data.encode('utf-8')).hexdigest()
        
        return {
            'report_date': date(2024, 5, 26),
            'report_type': section_type,
            'time_utc': time_str,
            'location': location,
            'county': county,
            'state': state,
            'latitude': latitude,
            'longitude': longitude,
            'magnitude': json.dumps(magnitude),
            'row_hash': row_hash,
            'ingested_at': datetime.utcnow(),
            'comments': comments,
            'raw_csv_line': line.strip()
        }
    except Exception as e:
        print(f"Error parsing {section_type} line {line_num}: {e}")
        return None

def comprehensive_may26_restoration():
    """Complete restoration ensuring all 849 reports are captured"""
    url = "https://www.spc.noaa.gov/climo/reports/240526_rpts_filtered.csv"
    
    print(f"Downloading CSV from {url}")
    response = requests.get(url)
    response.raise_for_status()
    
    lines = response.text.strip().split('\n')
    all_reports = []
    
    # Parse tornado section (lines 2-70)
    tornado_count = 0
    for i in range(1, 71):  # Skip header line 1
        if i < len(lines) and not lines[i].startswith('Time,'):
            report = parse_csv_line(lines[i], 'tornado', i+1)
            if report and len(report['state']) <= 2:  # Validate state field length
                all_reports.append(report)
                tornado_count += 1
    
    # Parse wind section (lines 72-711)
    wind_count = 0
    for i in range(71, 712):  # Skip header line 71
        if i < len(lines) and not lines[i].startswith('Time,'):
            report = parse_csv_line(lines[i], 'wind', i+1)
            if report and len(report['state']) <= 2:  # Validate state field length
                all_reports.append(report)
                wind_count += 1
    
    # Parse hail section (lines 713-852)
    hail_count = 0
    for i in range(712, len(lines)):  # Skip header line 712
        if i < len(lines) and not lines[i].startswith('Time,'):
            report = parse_csv_line(lines[i], 'hail', i+1)
            if report and len(report['state']) <= 2:  # Validate state field length
                all_reports.append(report)
                hail_count += 1
    
    print(f"Parsed reports: Tornado={tornado_count}, Wind={wind_count}, Hail={hail_count}")
    print(f"Total parsed: {len(all_reports)}")
    
    # Clear existing May 26 data
    print("Clearing existing May 26 data...")
    session.execute(text("DELETE FROM spc_reports WHERE report_date = '2024-05-26'"))
    session.commit()
    
    # Insert all reports with batch processing
    inserted_count = 0
    batch_size = 25  # Smaller batches to avoid constraint issues
    
    for i in range(0, len(all_reports), batch_size):
        batch = all_reports[i:i+batch_size]
        for report in batch:
            try:
                sql = text("""
                    INSERT INTO spc_reports (
                        report_date, report_type, time_utc, location, county, state,
                        latitude, longitude, magnitude, row_hash, ingested_at,
                        comments, raw_csv_line
                    ) VALUES (
                        :report_date, :report_type, :time_utc, :location, :county, :state,
                        :latitude, :longitude, :magnitude, :row_hash, :ingested_at,
                        :comments, :raw_csv_line
                    ) ON CONFLICT (row_hash) DO NOTHING
                """)
                
                session.execute(sql, report)
                inserted_count += 1
                
            except Exception as e:
                print(f"Error inserting report: {e}")
                session.rollback()
                continue
        
        # Commit each batch
        try:
            session.commit()
            print(f"Batch {i//batch_size + 1}: inserted {len(batch)} reports")
        except Exception as e:
            print(f"Batch commit failed: {e}")
            session.rollback()
    
    # Verify final count
    result = session.execute(text("""
        SELECT COUNT(*) as total_count,
               COUNT(CASE WHEN report_type = 'tornado' THEN 1 END) as tornado_count,
               COUNT(CASE WHEN report_type = 'wind' THEN 1 END) as wind_count,
               COUNT(CASE WHEN report_type = 'hail' THEN 1 END) as hail_count
        FROM spc_reports WHERE report_date = '2024-05-26'
    """))
    
    counts = result.fetchone()
    print(f"Final verification for May 26, 2024:")
    print(f"  Total: {counts.total_count}")
    print(f"  Tornado: {counts.tornado_count}")
    print(f"  Wind: {counts.wind_count}")
    print(f"  Hail: {counts.hail_count}")
    
    expected_total = 849
    if counts.total_count == expected_total:
        print(f"SUCCESS: Achieved complete restoration {counts.total_count}/{expected_total}")
    else:
        print(f"INCOMPLETE: Only restored {counts.total_count}/{expected_total} reports")
    
    return counts.total_count

if __name__ == "__main__":
    try:
        total = comprehensive_may26_restoration()
        print(f"May 26, 2024 comprehensive restoration result: {total} total reports")
    except Exception as e:
        print(f"Error in comprehensive restoration: {e}")
        sys.exit(1)
    finally:
        session.close()