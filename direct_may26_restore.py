#!/usr/bin/env python3
"""
Direct SQL restoration for May 26, 2024 - 849 reports
Uses raw SQL with proper CSV parsing to complete restoration
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

def direct_sql_restoration():
    """Direct SQL approach for complete May 26 restoration"""
    url = "https://www.spc.noaa.gov/climo/reports/240526_rpts_filtered.csv"
    
    print(f"Downloading May 26, 2024 CSV...")
    response = requests.get(url)
    response.raise_for_status()
    
    lines = response.text.strip().split('\n')
    print(f"Total lines in CSV: {len(lines)}")
    
    session = Session()
    
    try:
        # Clear existing data
        print("Clearing existing May 26 data...")
        session.execute(text("DELETE FROM spc_reports WHERE report_date = '2024-05-26'"))
        session.commit()
        
        # Insert each valid report with direct SQL
        insert_sql = text("""
            INSERT INTO spc_reports (
                report_date, report_type, time_utc, location, county, state,
                latitude, longitude, magnitude, row_hash, ingested_at, comments, raw_csv_line
            ) VALUES (
                '2024-05-26', :report_type, :time_utc, :location, :county, :state,
                :latitude, :longitude, :magnitude, :row_hash, now(), :comments, :raw_csv_line
            )
        """)
        
        tornado_count = 0
        wind_count = 0
        hail_count = 0
        
        # Process tornado section (lines 1-70, skip header at 0)
        for i in range(1, 71):
            if i < len(lines) and not lines[i].startswith('Time,'):
                fields = lines[i].strip().split(',')
                if len(fields) >= 5 and len(fields[4]) <= 2:  # Valid state field
                    tornado_count += 1
                    magnitude = json.dumps({'f_scale': fields[1] if len(fields) > 1 else ""})
                    
                    # Generate hash
                    hash_data = f"2024-05-26|tornado|{fields[0]}|{fields[2] if len(fields) > 2 else ''}|{fields[3] if len(fields) > 3 else ''}|{fields[4]}|{fields[5] if len(fields) > 5 else ''}|{fields[6] if len(fields) > 6 else ''}|{magnitude}"
                    row_hash = hashlib.sha256(hash_data.encode('utf-8')).hexdigest()
                    
                    session.execute(insert_sql, {
                        'report_type': 'tornado',
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
        
        print(f"Inserted {tornado_count} tornado reports")
        session.commit()
        
        # Process wind section (lines 72-711, skip header at 71)
        for i in range(72, 712):
            if i < len(lines) and not lines[i].startswith('Time,'):
                fields = lines[i].strip().split(',')
                if len(fields) >= 5 and len(fields[4]) <= 2:  # Valid state field
                    wind_count += 1
                    magnitude = json.dumps({'speed': fields[1] if len(fields) > 1 else ""})
                    
                    # Generate hash
                    hash_data = f"2024-05-26|wind|{fields[0]}|{fields[2] if len(fields) > 2 else ''}|{fields[3] if len(fields) > 3 else ''}|{fields[4]}|{fields[5] if len(fields) > 5 else ''}|{fields[6] if len(fields) > 6 else ''}|{magnitude}"
                    row_hash = hashlib.sha256(hash_data.encode('utf-8')).hexdigest()
                    
                    session.execute(insert_sql, {
                        'report_type': 'wind',
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
                    
                    # Commit every 100 wind reports to prevent timeout
                    if wind_count % 100 == 0:
                        session.commit()
                        print(f"Committed {wind_count} wind reports...")
        
        print(f"Inserted {wind_count} wind reports")
        session.commit()
        
        # Process hail section (lines 713+, skip header at 712)
        for i in range(713, len(lines)):
            if i < len(lines) and not lines[i].startswith('Time,'):
                fields = lines[i].strip().split(',')
                if len(fields) >= 5 and len(fields[4]) <= 2:  # Valid state field
                    hail_count += 1
                    magnitude = json.dumps({'size': fields[1] if len(fields) > 1 else ""})
                    
                    # Generate hash
                    hash_data = f"2024-05-26|hail|{fields[0]}|{fields[2] if len(fields) > 2 else ''}|{fields[3] if len(fields) > 3 else ''}|{fields[4]}|{fields[5] if len(fields) > 5 else ''}|{fields[6] if len(fields) > 6 else ''}|{magnitude}"
                    row_hash = hashlib.sha256(hash_data.encode('utf-8')).hexdigest()
                    
                    session.execute(insert_sql, {
                        'report_type': 'hail',
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
        
        print(f"Inserted {hail_count} hail reports")
        session.commit()
        
        total_inserted = tornado_count + wind_count + hail_count
        print(f"Total inserted: {total_inserted}")
        
        # Final verification
        result = session.execute(text("""
            SELECT COUNT(*) as total_count,
                   COUNT(CASE WHEN report_type = 'tornado' THEN 1 END) as tornado_count,
                   COUNT(CASE WHEN report_type = 'wind' THEN 1 END) as wind_count,
                   COUNT(CASE WHEN report_type = 'hail' THEN 1 END) as hail_count
            FROM spc_reports WHERE report_date = '2024-05-26'
        """))
        
        counts = result.fetchone()
        print(f"\nFINAL VERIFICATION:")
        print(f"Total: {counts.total_count}")
        print(f"Tornado: {counts.tornado_count}")
        print(f"Wind: {counts.wind_count}")
        print(f"Hail: {counts.hail_count}")
        
        if counts.total_count == 849:
            print("\n✓ SUCCESS: Achieved complete 849/849 restoration")
            return True
        else:
            print(f"\n✗ INCOMPLETE: Only {counts.total_count}/849 reports restored")
            return False
            
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
        return False
    finally:
        session.close()

if __name__ == "__main__":
    success = direct_sql_restoration()
    exit(0 if success else 1)