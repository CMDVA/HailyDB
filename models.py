from app import db
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Column, String, Text, DateTime, Boolean, func
from datetime import datetime

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

    # Metadata
    ingested_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

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
            'ingested_at': self.ingested_at.isoformat() if self.ingested_at else None
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
