from app import db
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Column, String, Text, DateTime, Date, Boolean, func, Index, UniqueConstraint
from datetime import datetime
import re

class Alert(db.Model):
    """
    NWS Alert model with full payload storage and enrichment fields
    Based on NWS Alert API schema: https://api.weather.gov/openapi.json
    """
    __tablename__ = "alerts"

    # Core NWS fields
    id = Column(String, primary_key=True)  # Same as properties.id from NWS
    event = Column(String, index=True)     # Event type (e.g., "Tornado Warning")
    severity = Column(String, index=True)  # Severity level
    area_desc = Column(Text)               # Area description
    effective = Column(DateTime)           # When alert becomes effective
    expires = Column(DateTime)             # When alert expires
    sent = Column(DateTime)                # When alert was sent
    
    # JSON storage for complex data
    geometry = Column(JSONB)               # Store full geometry block
    properties = Column(JSONB)             # Store all original NWS fields
    raw = Column(JSONB)                    # Entire feature object

    # AI Enrichment Fields
    ai_summary = Column(Text)              # AI-generated summary
    ai_tags = Column(JSONB)                # List of classified tags
    
    # SPC Cross-referencing
    spc_verified = Column(Boolean, default=False)
    spc_reports = Column(JSONB)            # List of matching SPC reports
    spc_confidence_score = Column(db.Float) # Match confidence (0.0-1.0)
    spc_match_method = Column(String(10))   # "fips", "latlon", "none"
    spc_report_count = Column(db.Integer, default=0)
    spc_ai_summary = Column(Text)          # AI-generated verification summary

    # Metadata
    ingested_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Database indexes for location-based queries and performance
    __table_args__ = (
        Index('idx_alert_area_desc', 'area_desc'),
        Index('idx_alert_severity', 'severity'),
        Index('idx_alert_event', 'event'),
        Index('idx_alert_effective', 'effective'),
        Index('idx_alert_expires', 'expires'),
        Index('idx_alert_ingested_at_desc', 'ingested_at'),  # Optimized for recent queries
        Index('idx_alert_active', 'effective', 'expires'),
        Index('idx_alert_spc_verified', 'spc_verified'),  # For verification tracking
    )

    def __repr__(self):
        return f'<Alert {self.id}: {self.event}>'
    
    def to_dict(self):
        """Convert alert to dictionary for JSON serialization"""
        return {
            'id': self.id,
            'event': self.event,
            'severity': self.severity,
            'area_desc': self.area_desc,
            'effective': self.effective.isoformat() if self.effective else None,
            'expires': self.expires.isoformat() if self.expires else None,
            'sent': self.sent.isoformat() if self.sent else None,
            'geometry': self.geometry,
            'properties': self.properties,
            'ai_summary': self.ai_summary,
            'ai_tags': self.ai_tags,
            'spc_verified': self.spc_verified,
            'spc_reports': self.spc_reports,
            'ingested_at': self.ingested_at.isoformat() if self.ingested_at else None,
            'is_active': self.is_active,
            'duration_minutes': self.duration_minutes,
            'location': self.get_location_info()
        }
    
    @property
    def is_active(self):
        """Check if alert is currently active"""
        now = datetime.utcnow()
        return (self.effective <= now if self.effective else True) and \
               (self.expires > now if self.expires else True)
    
    @property
    def duration_minutes(self):
        """Calculate alert duration in minutes"""
        if not self.effective or not self.expires:
            return None
        return int((self.expires - self.effective).total_seconds() / 60)
    
    def extract_states(self):
        """Extract state codes from area description"""
        if not self.area_desc:
            return []
        
        # Match state abbreviations (e.g., "FL", "CA", "TX")
        state_pattern = r'\b([A-Z]{2})\b'
        states = re.findall(state_pattern, self.area_desc)
        return list(set(states))
    
    def extract_counties(self):
        """Extract county names from area description"""
        if not self.area_desc:
            return []
        
        # Split by semicolon and extract county names before state codes
        areas = self.area_desc.split(';')
        counties = []
        
        for area in areas:
            area = area.strip()
            # Match pattern "County Name, ST"
            county_match = re.match(r'^([^,]+),\s*([A-Z]{2})$', area)
            if county_match:
                county_name = county_match.group(1).strip()
                # Remove " County" suffix if present
                county_name = re.sub(r'\s+County$', '', county_name)
                counties.append(county_name)
        
        return counties
    
    def get_location_info(self):
        """Get structured location information for API consumption"""
        return {
            'states': self.extract_states(),
            'counties': self.extract_counties(),
            'area_description': self.area_desc,
            'geocodes': self.properties.get('geocode', {}) if self.properties else {},
            'affected_zones': self.properties.get('affectedZones', []) if self.properties else []
        }

class IngestionLog(db.Model):
    """
    Log of ingestion attempts for monitoring and debugging
    """
    __tablename__ = "ingestion_logs"
    
    id = Column(db.Integer, primary_key=True)
    started_at = Column(DateTime, server_default=func.now())
    completed_at = Column(DateTime)
    success = Column(Boolean, default=False)
    alerts_processed = Column(db.Integer, default=0)
    new_alerts = Column(db.Integer, default=0)
    updated_alerts = Column(db.Integer, default=0)
    error_message = Column(Text)
    
    def __repr__(self):
        return f'<IngestionLog {self.id}: {self.success}>'


class SPCReport(db.Model):
    """
    Storm Prediction Center report model for verification
    Stores tornado, wind, and hail reports from SPC daily files
    """
    __tablename__ = "spc_reports"

    id = Column(db.Integer, primary_key=True)
    report_date = Column(Date, nullable=False, index=True)  # YYMMDD from filename
    report_type = Column(String(10), nullable=False, index=True)  # tornado, wind, hail
    time_utc = Column(String(4))  # HHMM format from CSV
    location = Column(String(100))
    county = Column(String(50), index=True)
    state = Column(String(2), index=True)
    latitude = Column(db.Float)
    longitude = Column(db.Float)
    comments = Column(Text)
    
    # Type-specific fields stored as JSON for flexibility
    magnitude = Column(JSONB)  # {f_scale: int} or {speed: int} or {size: int}
    
    # Tracking fields
    raw_csv_line = Column(Text)  # Store original CSV line for audit
    ingested_at = Column(DateTime, server_default=func.now())
    
    __table_args__ = (
        Index('idx_spc_date_type', 'report_date', 'report_type'),
        Index('idx_spc_location', 'state', 'county'),
        Index('idx_spc_coords', 'latitude', 'longitude'),
        # Use raw CSV line for true duplicate detection - multiple reports can occur at same time/location
        UniqueConstraint('report_date', 'raw_csv_line', name='uq_spc_report_csv_unique'),
    )

    def __repr__(self):
        return f'<SPCReport {self.report_type} {self.report_date} {self.location}>'

    def to_dict(self):
        """Convert SPC report to dictionary"""
        return {
            'id': self.id,
            'report_date': self.report_date.isoformat() if self.report_date else None,
            'report_type': self.report_type,
            'time_utc': self.time_utc,
            'location': self.location,
            'county': self.county,
            'state': self.state,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'magnitude': self.magnitude,
            'comments': self.comments,
            'ingested_at': self.ingested_at.isoformat() if self.ingested_at else None
        }


class SPCIngestionLog(db.Model):
    """
    Log of SPC report ingestion attempts
    """
    __tablename__ = "spc_ingestion_logs"

    id = Column(db.Integer, primary_key=True)
    report_date = Column(Date, nullable=False)
    started_at = Column(DateTime, server_default=func.now())
    completed_at = Column(DateTime)
    success = Column(Boolean, default=False)
    tornado_reports = Column(db.Integer, default=0)
    wind_reports = Column(db.Integer, default=0)
    hail_reports = Column(db.Integer, default=0)
    total_reports = Column(db.Integer, default=0)
    error_message = Column(Text)
    url_attempted = Column(String(200))

    def __repr__(self):
        return f'<SPCIngestionLog {self.report_date}: {self.success}>'

class SchedulerLog(db.Model):
    """
    Autonomous operation tracking for scheduler metadata
    Logs all automated ingestion attempts regardless of trigger method
    """
    __tablename__ = "scheduler_logs"
    
    id = Column(db.Integer, primary_key=True)
    operation_type = Column(String(50), nullable=False, index=True)  # 'nws_poll', 'spc_poll', 'spc_match'
    trigger_method = Column(String(20), nullable=False)  # 'manual', 'external_cron', 'internal_timer'
    started_at = Column(DateTime, server_default=func.now())
    completed_at = Column(DateTime)
    success = Column(Boolean, default=False)
    records_processed = Column(db.Integer, default=0)
    records_new = Column(db.Integer, default=0)
    error_message = Column(Text)
    operation_metadata = Column(JSONB)  # Operation-specific data
    
    def __repr__(self):
        return f'<SchedulerLog {self.id}: {self.operation_type} - {self.success}>'
