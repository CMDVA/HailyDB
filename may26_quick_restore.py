#!/usr/bin/env python3
"""
Quick May 26, 2024 restoration using direct SQL COPY for efficiency
"""
import os
import requests
import json
import hashlib
from datetime import datetime, date
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def quick_may26_restoration():
    """Quick restoration using bulk SQL operations"""
    url = "https://www.spc.noaa.gov/climo/reports/240526_rpts_filtered.csv"
    
    print(f"Downloading CSV from {url}")
    response = requests.get(url)
    response.raise_for_status()
    
    lines = response.text.strip().split('\n')
    
    # Clear existing May 26 data
    print("Clearing existing May 26 data...")
    session.execute(text("DELETE FROM spc_reports WHERE report_date = '2024-05-26'"))
    session.commit()
    
    # Process each section directly with SQL
    tornado_count = 0
    wind_count = 0
    hail_count = 0
    
    # Process tornado section (lines 1-70, skip header at line 0)
    for i in range(1, 71):
        if i < len(lines) and not lines[i].startswith('Time,'):
            fields = lines[i].strip().split(',')
            if len(fields) >= 5 and len(fields[4]) <= 2:
                tornado_count += 1
                magnitude = json.dumps({'f_scale': fields[1] if len(fields) > 1 else ""})
                hash_data = f"2024-05-26|tornado|{fields[0]}|{fields[2] if len(fields) > 2 else ''}|{fields[3] if len(fields) > 3 else ''}|{fields[4]}|{fields[5] if len(fields) > 5 else ''}|{fields[6] if len(fields) > 6 else ''}|{magnitude}"
                row_hash = hashlib.sha256(hash_data.encode('utf-8')).hexdigest()
                
                session.execute(text("""
                    INSERT INTO spc_reports (
                        report_date, report_type, time_utc, location, county, state,
                        latitude, longitude, magnitude, row_hash, ingested_at,
                        comments, raw_csv_line
                    ) VALUES (
                        '2024-05-26', 'tornado', :time_utc, :location, :county, :state,
                        :latitude, :longitude, :magnitude, :row_hash, now(),
                        :comments, :raw_csv_line
                    )
                """), {
                    'time_utc': fields[0],
                    'location': fields[2] if len(fields) > 2 else "",
                    'county': fields[3] if len(fields) > 3 else "",
                    'state': fields[4],
                    'latitude': float(fields[5]) if len(fields) > 5 and fields[5] else None,
                    'longitude': float(fields[6]) if len(fields) > 6 and fields[6] else None,
                    'magnitude': magnitude,
                    'row_hash': row_hash,
                    'comments': fields[7] if len(fields) > 7 else "",
                    'raw_csv_line': lines[i].strip()
                })
    
    # Process wind section (lines 71-711, skip header at line 71)
    for i in range(72, 712):
        if i < len(lines) and not lines[i].startswith('Time,'):
            fields = lines[i].strip().split(',')
            if len(fields) >= 5 and len(fields[4]) <= 2:
                wind_count += 1
                magnitude = json.dumps({'speed': fields[1] if len(fields) > 1 else ""})
                hash_data = f"2024-05-26|wind|{fields[0]}|{fields[2] if len(fields) > 2 else ''}|{fields[3] if len(fields) > 3 else ''}|{fields[4]}|{fields[5] if len(fields) > 5 else ''}|{fields[6] if len(fields) > 6 else ''}|{magnitude}"
                row_hash = hashlib.sha256(hash_data.encode('utf-8')).hexdigest()
                
                session.execute(text("""
                    INSERT INTO spc_reports (
                        report_date, report_type, time_utc, location, county, state,
                        latitude, longitude, magnitude, row_hash, ingested_at,
                        comments, raw_csv_line
                    ) VALUES (
                        '2024-05-26', 'wind', :time_utc, :location, :county, :state,
                        :latitude, :longitude, :magnitude, :row_hash, now(),
                        :comments, :raw_csv_line
                    )
                """), {
                    'time_utc': fields[0],
                    'location': fields[2] if len(fields) > 2 else "",
                    'county': fields[3] if len(fields) > 3 else "",
                    'state': fields[4],
                    'latitude': float(fields[5]) if len(fields) > 5 and fields[5] else None,
                    'longitude': float(fields[6]) if len(fields) > 6 and fields[6] else None,
                    'magnitude': magnitude,
                    'row_hash': row_hash,
                    'comments': fields[7] if len(fields) > 7 else "",
                    'raw_csv_line': lines[i].strip()
                })
    
    # Process hail section (lines 712+, skip header at line 712)
    for i in range(713, len(lines)):
        if i < len(lines) and not lines[i].startswith('Time,'):
            fields = lines[i].strip().split(',')
            if len(fields) >= 5 and len(fields[4]) <= 2:
                hail_count += 1
                magnitude = json.dumps({'size': fields[1] if len(fields) > 1 else ""})
                hash_data = f"2024-05-26|hail|{fields[0]}|{fields[2] if len(fields) > 2 else ''}|{fields[3] if len(fields) > 3 else ''}|{fields[4]}|{fields[5] if len(fields) > 5 else ''}|{fields[6] if len(fields) > 6 else ''}|{magnitude}"
                row_hash = hashlib.sha256(hash_data.encode('utf-8')).hexdigest()
                
                session.execute(text("""
                    INSERT INTO spc_reports (
                        report_date, report_type, time_utc, location, county, state,
                        latitude, longitude, magnitude, row_hash, ingested_at,
                        comments, raw_csv_line
                    ) VALUES (
                        '2024-05-26', 'hail', :time_utc, :location, :county, :state,
                        :latitude, :longitude, :magnitude, :row_hash, now(),
                        :comments, :raw_csv_line
                    )
                """), {
                    'time_utc': fields[0],
                    'location': fields[2] if len(fields) > 2 else "",
                    'county': fields[3] if len(fields) > 3 else "",
                    'state': fields[4],
                    'latitude': float(fields[5]) if len(fields) > 5 and fields[5] else None,
                    'longitude': float(fields[6]) if len(fields) > 6 and fields[6] else None,
                    'magnitude': magnitude,
                    'row_hash': row_hash,
                    'comments': fields[7] if len(fields) > 7 else "",
                    'raw_csv_line': lines[i].strip()
                })
    
    # Commit all changes
    session.commit()
    
    print(f"Inserted: Tornado={tornado_count}, Wind={wind_count}, Hail={hail_count}")
    
    # Final verification
    result = session.execute(text("""
        SELECT COUNT(*) as total_count,
               COUNT(CASE WHEN report_type = 'tornado' THEN 1 END) as tornado_count,
               COUNT(CASE WHEN report_type = 'wind' THEN 1 END) as wind_count,
               COUNT(CASE WHEN report_type = 'hail' THEN 1 END) as hail_count
        FROM spc_reports WHERE report_date = '2024-05-26'
    """))
    
    counts = result.fetchone()
    print(f"Final verification: Total={counts.total_count}, Tornado={counts.tornado_count}, Wind={counts.wind_count}, Hail={counts.hail_count}")
    
    if counts.total_count == 849:
        print("SUCCESS: Achieved complete 849/849 restoration")
        return True
    else:
        print(f"INCOMPLETE: Only {counts.total_count}/849 reports restored")
        return False

if __name__ == "__main__":
    try:
        success = quick_may26_restoration()
        if success:
            print("May 26, 2024 restoration completed successfully")
        else:
            print("May 26, 2024 restoration incomplete")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        session.close()