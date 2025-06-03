import os

class Config:
    """Configuration settings for NWS Alert Ingestion Service"""
    
    # NWS API Configuration
    NWS_ALERT_URL = "https://api.weather.gov/alerts/active"
    NWS_HEADERS = {
        'User-Agent': 'HailyDB-NWS-Ingestion/1.0 (contact@hailydb.com)',
        'Accept': 'application/geo+json'
    }
    
    # Database Configuration
    DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/nws_alerts")
    
    # OpenAI Configuration
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
    
    # Ingestion Settings
    POLLING_INTERVAL_MINUTES = 5
    BATCH_SIZE = 100
    REQUEST_TIMEOUT = 30
    
    # SPC Integration (Future)
    SPC_REPORTS_URL = "https://www.spc.noaa.gov/climo/reports/"
    
    # Logging Configuration
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    
    @classmethod
    def validate(cls):
        """Validate configuration settings"""
        if not cls.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        
        if not cls.DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable is required")
        
        return True
