import json
import logging
from typing import List, Dict, Optional, Union
from kafka import KafkaProducer

from observation_scraper.models.climate_data import KafkaBeat
from observation_scraper.config.loader import Config

logger = logging.getLogger(__name__)

class ObservationProducer:
    def __init__(self, bootstrap_servers: Optional[Union[List[str], str]] = None):
        """
        Initialize the Kafka producer.
        
        Args:
            bootstrap_servers: List of Kafka bootstrap servers or comma-separated string
        """

        config = Config()
        kafka_config = config.kafka_config

        if bootstrap_servers is None:
            bootstrap_servers = kafka_config.get('bootstrap_servers', 'localhost:9092')

        if isinstance(bootstrap_servers, str):
            bootstrap_servers = bootstrap_servers.split(',')

        self.bootstrap_servers = bootstrap_servers
        self.defalt_topic = bootstrap_servers
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
    
    def send_beat(self, topic: str, beat: KafkaBeat, key: str = None) -> None:
        """
        Send a single beat to Kafka.
        
        Args:
            topic: Kafka topic name
            beat: KafkaBeat object to send
            key: Optional message key
        """
        # Convert the beat to a dictionary
        beat_dict = beat.model_dump()
        
        # Send to Kafka
        self.producer.send(topic, key=key, value=beat_dict)
    
    def close(self) -> None:
        """Close the Kafka producer."""
        if self.producer:
            self.producer.close()
