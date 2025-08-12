from typing import Optional
from datetime import datetime
from pydantic import BaseModel, validator

class CLIData(BaseModel):

    report_date: Optional[str] = None
    report_datetime: Optional[datetime] = None
    station_id: str
    
    # Essential temperature data - non-optional
    temperature_max: float
    temperature_min: float
    temperature_avg: float
    
    # Time data for temperature extremes - non-optional
    temperature_max_time: str
    temperature_min_time: str
    temperature_max_datetime: datetime
    temperature_min_datetime: datetime
    
    # Other measurements - optional
    precipitation_yesterday: Optional[float] = 0.0
    
    humidity_highest: Optional[int] = None
    humidity_lowest: Optional[int] = None
    humidity_avg: Optional[int] = None
    
    wind_highest_speed: Optional[float] = None
    wind_highest_direction: Optional[str] = None
    wind_avg_speed: Optional[float] = None
    
    # Validate temperature values
    @validator('temperature_max', 'temperature_min', 'temperature_avg')
    def check_temperature_values(cls, v):
        if v < -150 or v > 150:  # Reasonable range for Earth temperatures in F
            raise ValueError(f"Temperature value {v} is outside reasonable range")
        return v
    
    # Validate precipitation values
    @validator('precipitation_yesterday')
    def check_precipitation_values(cls, v):
        if v is not None and v < 0:
            raise ValueError(f"Precipitation value cannot be negative: {v}")
        return v
    
    # Validate humidity values
    @validator('humidity_highest', 'humidity_lowest', 'humidity_avg')
    def check_humidity_values(cls, v):
        if v is not None and (v < 0 or v > 100):
            raise ValueError(f"Humidity value must be between 0 and 100, got {v}")
        return v
    
    # Validate wind speed values
    @validator('wind_highest_speed', 'wind_avg_speed')
    def check_wind_speed_values(cls, v):
        if v is not None and v < 0:
            raise ValueError(f"Wind speed value cannot be negative: {v}")
        return v

class KafkaBeat(BaseModel):
    """Model for Kafka beat messages with essential weather data."""
    
    # Required fields
    measurement_type: str
    value: float
    unit: str
    observation_type: str
    timestamp: str
    station_id: str
    service: str
    
    # Optional field for averages
    period: Optional[str] = None
