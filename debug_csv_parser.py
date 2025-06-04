#!/usr/bin/env python3
"""
Debug script to analyze March 15th SPC CSV parsing failures
Identifies specific malformation patterns causing data loss
"""
import requests
from datetime import date

def analyze_march_15_csv():
    """Download and analyze March 15th CSV for parsing failures"""
    url = "https://www.spc.noaa.gov/climo/reports/250315_rpts_filtered.csv"
    response = requests.get(url)
    csv_content = response.text
    
    lines = csv_content.strip().split('\n')
    print(f"Total lines in CSV: {len(lines)}")
    
    # Count data records vs headers
    data_lines = []
    header_lines = []
    malformed_lines = []
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
            
        if 'Time,' in line and ('F_Scale' in line or 'Speed' in line or 'Size' in line):
            header_lines.append((i+1, line))
        elif len(line) >= 4 and line[:4].isdigit():
            data_lines.append((i+1, line))
        else:
            malformed_lines.append((i+1, line))
    
    print(f"Headers found: {len(header_lines)}")
    print(f"Data records found: {len(data_lines)}")
    print(f"Malformed lines: {len(malformed_lines)}")
    
    # Analyze data line structures
    comma_counts = {}
    for line_num, line in data_lines:
        comma_count = line.count(',')
        if comma_count not in comma_counts:
            comma_counts[comma_count] = []
        comma_counts[comma_count].append((line_num, line[:100]))
    
    print("\nComma count distribution:")
    for count in sorted(comma_counts.keys()):
        print(f"  {count} commas: {len(comma_counts[count])} lines")
        if count < 7:  # Show examples of problematic lines
            for line_num, sample in comma_counts[count][:3]:
                print(f"    Line {line_num}: {sample}")
    
    # Show malformed lines
    print(f"\nMalformed lines ({len(malformed_lines)}):")
    for line_num, line in malformed_lines[:10]:
        print(f"  Line {line_num}: {line[:100]}")
    
    return len(data_lines), malformed_lines

if __name__ == "__main__":
    analyze_march_15_csv()