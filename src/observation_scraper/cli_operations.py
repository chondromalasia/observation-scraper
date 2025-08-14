from typing import List, Optional
from datetime import datetime

from observation_scraper.config.loader import Config
from observation_scraper.scrapers.cli import CLIScraper
from observation_scraper.parsers.cli import CLIParser
from observation_scraper.models.climate_data import CLIData, KafkaBeat
from observation_scraper.producers.producer import ObservationProducer

def get_climate_data(location_key: str) -> CLIData:
    """
    Get climate data for a specific location.
    
    Args:
        location_key: The key of the location in the configuration.
        
    Returns:
        A CLIData object containing structured climate data.
        
    Raises:
        ValueError: If the report doesn't contain yesterday's data.
        requests.RequestException: For various request-related errors.
    """
    # Load configuration
    config = Config()
    
    # Initialize scraper and parser
    cli_config = config.cli_config['cli']
    scraper = CLIScraper(cli_config)
    parser = CLIParser()
    
    # Fetch the report
    html_content = scraper.fetch_report(location_key)
    
    # Extract the report content
    report_content = parser.extract_report_content(html_content)
    
    # Parse the report
    climate_data = parser.parse_report(report_content, location_key)
    
    return climate_data

def format_kafka_beats(cli_data: CLIData) -> List[KafkaBeat]:
    """
    Format climate data into Kafka beats.
    
    Args:
        cli_data: The parsed climate data.
        
    Returns:
        A list of KafkaBeat objects formatted for Kafka.
    """
    beats = []
    
    # Helper function to create a Kafka beat
    def create_beat(measurement_type, value, unit, observation_type, timestamp, period=None):
        return KafkaBeat(
            measurement_type=measurement_type,
            value=value,
            unit=unit,
            observation_type=observation_type,
            timestamp=timestamp,
            period=period,
            station_id=cli_data.station_id,
            service="CLI"
        )
    
    # Create end-of-day timestamp if we have a report date
    end_of_day_timestamp = None
    if cli_data.report_datetime:
        end_of_day = datetime(
            cli_data.report_datetime.year,
            cli_data.report_datetime.month,
            cli_data.report_datetime.day,
            23, 59, 59
        )
        end_of_day_timestamp = end_of_day.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Add instantaneous measurements (with their exact timestamps)
    
    # Temperature max
    max_temp_timestamp = cli_data.temperature_max_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    beats.append(create_beat("temperature", cli_data.temperature_max, "F", "max", max_temp_timestamp))
    
    # Temperature min
    min_temp_timestamp = cli_data.temperature_min_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    beats.append(create_beat("temperature", cli_data.temperature_min, "F", "min", min_temp_timestamp))
    
    # Add period-based measurements (if we have a report date)
    if end_of_day_timestamp:
        # Temperature average
        beats.append(create_beat(
            "temperature", cli_data.temperature_avg, "F", "average", 
            end_of_day_timestamp, period="daily"
        ))
        
        # Precipitation (if available)
        if cli_data.precipitation_yesterday is not None:
            beats.append(create_beat(
                "precipitation", cli_data.precipitation_yesterday, "IN", "total", 
                end_of_day_timestamp, period="daily"
            ))
        
        # Humidity average (if available)
        if cli_data.humidity_avg is not None:
            beats.append(create_beat(
                "humidity", cli_data.humidity_avg, "%", "average", 
                end_of_day_timestamp, period="daily"
            ))
        
        # Wind speed (if available)
        if cli_data.wind_highest_speed is not None:
            beats.append(create_beat(
                "wind_speed", cli_data.wind_highest_speed, "MPH", "max", 
                end_of_day_timestamp
            ))
    
    return beats

def publish_beats_to_kafka(
    beats: List[KafkaBeat], 
    topic: Optional[str] = None
) -> int:
    """
    Publish a list of beats to Kafka using configuration.
    
    Args:
        beats: List of KafkaBeat objects
        topic: Optional topic override. If None, uses default from config.
    
    Returns:
        Number of beats published
    """
    producer = ObservationProducer()
    try:
        count = 0
        for beat in beats:
            producer.send_beat(beat, topic=topic)
            count += 1
        producer.producer.flush()
        return count
    finally:
        producer.close()

def get_and_publish_kafka_beats(location_key: str, topic: str) -> int:
    """
    Get Kafka beats for a specific location.
    
    Args:
        location_key: The key of the location in the configuration.
        
    Returns:
        A list of KafkaBeat objects ready to be sent to Kafka.
    """
    # Get the climate data
    climate_data = get_climate_data(location_key)
    
    # Format the data for Kafka
    beats = format_kafka_beats(climate_data)
    
    return publish_beats_to_kafka(beats, topic=topic)

def create_test_beat() -> KafkaBeat:
    """Create a test weather observation beat."""
    return KafkaBeat(
        measurement_type="temperature",
        value=22.5,
        unit="celsius", 
        observation_type="test_deployment",
        timestamp=datetime.utcnow().isoformat() + "Z",
        station_id="DEPLOYMENT_TEST_STATION",
        service="kubernetes_deployment_test",
        period="test"
    )

def send_dummy_beat():

    config = Config().kafka_config

    dummy_beats = list(create_test_beat())

    topic = config.get('topic')

    return publish_beats_to_kafka(beats=dummy_beats, topic='observations')
    
