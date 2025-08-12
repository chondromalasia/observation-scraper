import unittest
from unittest.mock import Mock, patch
from observation_scraper.producers.producer import ObservationProducer
from observation_scraper.models.climate_data import KafkaBeat


class TestObservationProducer(unittest.TestCase):
    """Simple test to verify producer is configured correctly."""
    
    @patch('observation_scraper.producers.producer.Config')
    @patch('observation_scraper.producers.producer.KafkaProducer')
    def test_producer_can_send_message(self, mock_kafka_producer, mock_config):
        """Test that producer can initialize and send a message."""
        # Mock config
        mock_config_instance = Mock()
        mock_config_instance.kafka_config = {'bootstrap_servers': 'localhost:9092'}
        mock_config.return_value = mock_config_instance
        
        # Mock KafkaProducer
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance
        
        # Create test data
        test_beat = KafkaBeat(
            measurement_type="temperature",
            value=25.0,
            unit="celsius",
            observation_type="current",
            timestamp="2024-01-15T10:30:00Z",
            station_id="TEST_STATION",
            service="test_service"
        )
        
        # Initialize producer and send message
        producer = ObservationProducer()
        producer.send_beat("test_topic", test_beat, "test_key")
        
        # Verify the message was sent
        mock_producer_instance.send.assert_called_once()
        call_args = mock_producer_instance.send.call_args
        
        # Basic assertions
        self.assertEqual(call_args[0][0], "test_topic")  # topic
        self.assertEqual(call_args[1]["key"], "test_key")  # key
        self.assertIn("measurement_type", call_args[1]["value"])  # message contains data
        
        producer.close()

class TestObservationProducerIntegration(unittest.TestCase):
    """Integration test against real Kafka."""
    
    def test_real_kafka_send(self):
        """Test sending to actual Kafka instance (requires docker-compose up)."""
        # Create test data
        test_beat = KafkaBeat(
            measurement_type="temperature",
            value=23.5,
            unit="celsius",
            observation_type="current",
            timestamp="2024-01-15T12:00:00Z",
            station_id="INTEGRATION_STATION",
            service="integration_test"
        )
        
        producer = None
        try:
            # Use localhost:9092 from your docker-compose
            producer = ObservationProducer(bootstrap_servers="localhost:9092")
            
            # Send message - note: key needs to be bytes or None
            producer.send_beat("test_topic", test_beat, key="integration_key".encode('utf-8'))
            
            # If we get here without exception, it worked!
            print("✅ Message sent successfully to Kafka!")
            
        except Exception as e:
            self.fail(f"Integration test failed: {e}")
        finally:
            if producer:
                producer.close()


if __name__ == '__main__':
    unittest.main()
