# SPC Storm Report Ingestion - Data Organization and Timezone Handling

## SPC Report Organization

The Storm Prediction Center organizes storm reports based on **meteorological days**, not calendar days:

- **Report Period**: 1200 UTC to 1159 UTC the next day
- **Example**: The report file `240603_rpts_filtered.csv` covers:
  - From: 2024-06-03 at 12:00 UTC
  - To: 2024-06-04 at 11:59 UTC

## Timezone Considerations

### US Time Zones vs UTC
- **Florida (UTC-4)**: A storm at 0010 UTC (12:10 AM) is actually 8:10 PM the previous day
- **Central Time (UTC-5)**: Same storm would be 7:10 PM the previous day  
- **Hawaii (UTC-10)**: Same storm would be 2:10 PM the previous day

### Current HailyDB Approach
- **Storage**: All times stored in UTC (no timezone conversion)
- **Date Assignment**: Events are assigned to the SPC meteorological day
- **User Requirement**: Last 3 days (Today-4) synced for Florida user wake-up

## Data Integrity Issue

**Problem**: Events occurring between 00:00-11:59 UTC are meteorologically correct but may appear on the "wrong" calendar day for US users.

**Example from 250603_rpts_filtered.csv**:
```
0010,UNK,1 SW Coyne Center,Rock Island,IL,41.39,-90.59,Tornado tracked from southwest...
```
This tornado at 00:10 UTC is:
- **SPC Date**: 2024-06-03 (correct meteorologically)
- **Local Time**: 2024-06-02 at 8:10 PM Eastern (previous calendar day)

## Current Implementation Status

**Need to Verify**: Are we correctly handling the SPC meteorological day assignment, or are we incorrectly assigning events to calendar days?

### Expected Behavior
1. All reports in `240603_rpts_filtered.csv` should be assigned `report_date = '2024-06-03'`
2. Times 0000-1159 UTC are meteorologically part of the SPC day
3. Times 1200-2359 UTC are also part of the same SPC day

### Ingestion Schedule for Florida User (UTC-4)
- **Target**: Last 3 days available upon wake-up
- **Implementation**: Today-4 sync ensures adequate coverage
- **Frequency**: 
  - Today: Every 5 minutes
  - Last 3 days: Every 3 hours
  - Historical: Daily

## Current Implementation Status ✓

**VERIFIED**: HailyDB correctly implements SPC meteorological day assignment.

### Confirmed Behavior
- Events at 00:01-11:59 UTC are correctly assigned to the SPC meteorological day
- Example: Storm at 00:10 UTC on June 4th is properly stored as `report_date = '2025-06-04'`
- All reports from `YYMMDD_rpts_filtered.csv` are assigned to that date regardless of UTC time
- This matches SPC's 12:00 UTC to 11:59 UTC next day reporting period

### US-Focused Implementation
- **Geographic Scope**: United States only
- **Time Storage**: All times remain in UTC (no conversion needed)
- **Date Assignment**: Follows SPC meteorological day convention
- **User Experience**: Florida user (UTC-4) sees meteorologically correct storm dates

### Systematic Polling Schedule

**Implemented Schedule**:
- **T-0 (Today)**: Every 5 minutes - Real-time current day updates
- **T-1 through T-4**: Hourly updates on the hour - Recent critical period  
- **T-5 through T-7**: Every 3 hours - Recent historical period
- **T-8 through T-15**: Daily updates - Stabilizing historical period
- **T-16+**: Data protected - No automatic polling (backfill only)

**Florida User Optimization**:
- Morning wake-up guaranteed fresh T-1 through T-4 data
- Hourly updates ensure data completeness for critical recent period
- Systematic coverage eliminates polling gaps

**Data Protection**:
- T-16+ dates protected from automatic updates
- Backfill processing available for missing data recovery
- Manual override capability for data corrections