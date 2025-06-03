# HailyDB v2.0 - Core Stability Upgrade

## Overview

HailyDB is a comprehensive National Weather Service (NWS) Alert Ingestion Platform designed for real-time weather data processing and verification. The v2.0 core upgrade implements enhanced stability, autonomous operation monitoring, and comprehensive data integrity verification.

## System Architecture

### Core Components

- **Flask Backend**: RESTful API with SQLAlchemy ORM
- **PostgreSQL Database**: Comprehensive alert storage with JSONB fields
- **NWS Alert Ingestion**: Real-time polling from weather.gov APIs
- **SPC Report Integration**: Storm Prediction Center data cross-referencing
- **AI Enrichment**: OpenAI-powered alert summarization and tagging
- **Admin Dashboard**: Real-time monitoring and control interface

### Key Features

✅ **Autonomous Ingestion**: Self-sustaining data collection with error recovery
✅ **Data Integrity Verification**: Cross-reference NWS alerts with actual SPC reports
✅ **Comprehensive Monitoring**: Operation logging and health status tracking
✅ **Geographic Intelligence**: County FIPS and proximity-based alert matching
✅ **Production Ready**: Robust error handling and session management

## Database Schema

### Core Tables

#### `alerts` - NWS Alert Storage
- Full NWS alert payload with JSONB storage
- AI-generated summaries and tags
- SPC verification fields and confidence scores
- Geographic and temporal indexing

#### `spc_reports` - Storm Prediction Center Data
- Tornado, wind, and hail reports from SPC CSV files
- Geographic coordinates and FIPS county mapping
- Original CSV line preservation for audit trails

#### `scheduler_logs` - Operation Tracking
- Comprehensive logging of all ingestion operations
- Success/failure tracking with error messages
- Trigger method identification (manual, external, timer)

## API Endpoints

### Public API
- `GET /alerts` - Query alerts with filtering
- `GET /alerts/{id}` - Individual alert details
- `GET /alerts/summaries` - AI-generated summaries
- `GET /spc/reports` - Storm Prediction Center reports

### Internal Monitoring
- `GET /internal/status` - Comprehensive health status
- `GET /internal/dashboard` - Administrative interface
- `POST /internal/spc-reupload/{date}` - Force SPC data re-ingestion

### Health Status Response
```json
{
  "status": "healthy",
  "timestamp": "2025-06-03T19:03:30.123456",
  "database": "healthy",
  "alerts": {
    "total": 393,
    "recent_24h": 393,
    "active_now": 15
  },
  "spc_verification": {
    "verified_count": 0,
    "unverified_count": 393,
    "coverage_percentage": 0.0,
    "oldest_unverified": "2025-06-01T11:30:00"
  },
  "ingestion": {
    "last_nws_ingestion": "2025-06-03T00:14:52.597190",
    "last_spc_ingestion": "2025-06-03T15:46:38.366969",
    "failed_jobs_24h": 0
  },
  "scheduler_operations": {
    "total_operations_24h": 0,
    "successful_operations_24h": 0,
    "failed_operations_24h": 0
  }
}
```

## Data Sources

### NWS Alert API
- **Endpoint**: `https://api.weather.gov/alerts/active`
- **Format**: GeoJSON with comprehensive alert metadata
- **Polling**: Manual triggers via web interface
- **Coverage**: Nationwide real-time weather alerts

### SPC Storm Reports
- **Endpoint**: `https://www.spc.noaa.gov/climo/reports/YYMMDD_rpts_filtered.csv`
- **Format**: Multi-section CSV (tornado, wind, hail)
- **Coverage**: Historical storm verification data
- **Matching**: Geographic proximity and temporal correlation

## Deployment

### Environment Requirements
- Python 3.11+
- PostgreSQL database
- OpenAI API key for enrichment

### Required Environment Variables
```bash
DATABASE_URL=postgresql://user:password@host:port/database
OPENAI_API_KEY=sk-...
SESSION_SECRET=your-session-secret
```

### Installation
```bash
# Dependencies are managed via pyproject.toml
pip install -e .

# Database initialization is automatic on first run
python main.py
```

### Server Startup
```bash
# Development
python main.py

# Production
gunicorn --bind 0.0.0.0:5000 --reuse-port --reload main:app
```

## Data Integrity Features

### SPC Verification Process
1. **Geographic Matching**: County FIPS codes and 25-mile radius proximity
2. **Temporal Correlation**: ±2 hour window from alert effective time
3. **Event Type Mapping**: Tornado → Warning/Watch, Wind/Hail → Severe T-storm
4. **Confidence Scoring**: 0.9 for FIPS match, 0.7 for proximity match

### Error Recovery
- Automatic session rollback on database errors
- Comprehensive operation logging for audit trails
- Graceful degradation on API failures
- Duplicate detection using full CSV content comparison

### Data Quality Assurance
- Null character sanitization for PostgreSQL compatibility
- UNK value preservation for incomplete storm reports
- Transaction-safe re-upload functionality
- Comprehensive logging of all data modifications

## Monitoring and Diagnostics

### Real-time Metrics
- Alert ingestion rates and success ratios
- SPC verification coverage percentages
- Active alert counts and geographic distribution
- Database health and connection status

### Administrative Controls
- Manual trigger system for precise operation control
- Force re-ingestion capabilities for data correction
- Comprehensive error logging and recovery tracking
- Historical operation statistics and trends

## Architecture Decisions

### Why Manual Triggers Over APScheduler
- **Visibility**: Real-time monitoring of all operations
- **Control**: Precise timing and resource management
- **Reliability**: No scheduler conflicts in containerized environments
- **Recovery**: Easy intervention and correction capabilities

### Why PostgreSQL JSONB
- **Flexibility**: Store complete NWS payloads without schema constraints
- **Performance**: Indexed JSON queries for complex filtering
- **Integrity**: ACID compliance with complex nested data
- **Scalability**: Efficient storage and retrieval of large documents

### Why Current Coordinate Math Over External Libraries
- **Performance**: Direct haversine calculations without library overhead
- **Simplicity**: Deterministic and explainable geographic matching
- **Reliability**: No external dependencies for critical functionality
- **Maintenance**: Self-contained logic with clear debugging capabilities

## Production Readiness

✅ **Error Handling**: Comprehensive exception catching and logging
✅ **Session Management**: Proper database transaction handling
✅ **Data Validation**: Input sanitization and type checking
✅ **Performance Optimization**: Strategic database indexing
✅ **Monitoring**: Health checks and operation tracking
✅ **Documentation**: Complete API and architecture documentation

## Support and Maintenance

### Log Analysis
All operations are logged with timestamps, success indicators, and detailed error messages. Monitor `/internal/status` for real-time system health.

### Data Recovery
Force re-ingestion capabilities allow correction of data integrity issues. Use SPC verification dashboard to identify and resolve mismatches.

### Performance Tuning
Database indexes are optimized for common query patterns. Monitor query performance and adjust indexing strategy as data volume grows.

---

**HailyDB v2.0** - Production-ready weather data ingestion platform with comprehensive monitoring and data integrity verification.