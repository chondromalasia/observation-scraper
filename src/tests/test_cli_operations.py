import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

from observation_scraper.cli_operations import get_climate_data, format_kafka_beats
from observation_scraper.models.climate_data import CLIData, KafkaBeat

class TestCLIOperations(unittest.TestCase):

    def test_imports(self):
        self.assertIsNotNone(get_climate_data)

    def setUp(self):
        """Set up test fixtures."""
        # Create a sample CLIData object for testing
        self.test_data = CLIData(
            report_date="APRIL 17 2025",
            report_datetime=datetime(2025, 4, 17),
            station_id="KNYC",
            temperature_max=64,
            temperature_min=42,
            temperature_avg=53,
            temperature_max_time="3:41 PM",
            temperature_min_time="5:42 AM",
            temperature_max_datetime=datetime(2025, 4, 17, 15, 41),
            temperature_min_datetime=datetime(2025, 4, 17, 5, 42),
            precipitation_yesterday=0.00,
            humidity_highest=55,
            humidity_lowest=15,
            humidity_avg=35,
            wind_highest_speed=17,
            wind_highest_direction="NW",
            wind_avg_speed=6.6
        )
        
        # Sample HTML content
        self.sample_html = """
        <!DOCTYPE html><html>
        <body>
        <pre class="glossaryProduct">
        CLIMATE REPORT
        NATIONAL WEATHER SERVICE NEW YORK, NY
        217 AM EDT FRI APR 18 2025

        ...THE CENTRAL PARK NY CLIMATE SUMMARY FOR APRIL 17 2025...

        TEMPERATURE (F)
         YESTERDAY
          MAXIMUM         64    341 PM  96    2002  63
          MINIMUM         42    542 AM  27    1875  46
          AVERAGE         53
        </pre>
        </body>
        </html>
        """
    
    def test_get_climate_data(self):
        """Test the get_climate_data function."""
        # Arrange
        location_key = "KNYC"
        
        # Mock the dependencies
        with patch('observation_scraper.scrapers.cli.CLIScraper.fetch_report', return_value=self.sample_html):
            with patch('observation_scraper.parsers.cli.CLIParser.parse_report', return_value=self.test_data):
                
                # Act
                result = get_climate_data(location_key)
                
                # Assert
                self.assertEqual(result, self.test_data)
                self.assertEqual(result.station_id, "KNYC")
                self.assertEqual(result.temperature_max, 64)
                self.assertEqual(result.temperature_min, 42)

    def test_format_kafka_beats(self):
        """Test the format_kafka_beats function."""
        # Act
        beats = format_kafka_beats(self.test_data)
        
        # Assert
        self.assertIsInstance(beats, list)
        self.assertTrue(len(beats) >= 3)  # At least temperature max, min, and avg
        
        # Check temperature max beat
        temp_max_beat = next(b for b in beats if b.measurement_type == "temperature" and b.observation_type == "max")
        self.assertEqual(temp_max_beat.value, 64)
        self.assertEqual(temp_max_beat.unit, "F")
        self.assertEqual(temp_max_beat.timestamp, "2025-04-17T15:41:00Z")
        self.assertEqual(temp_max_beat.station_id, "KNYC")
        
        # Check temperature min beat
        temp_min_beat = next(b for b in beats if b.measurement_type == "temperature" and b.observation_type == "min")
        self.assertEqual(temp_min_beat.value, 42)
        self.assertEqual(temp_min_beat.timestamp, "2025-04-17T05:42:00Z")
        
        # Check temperature avg beat
        temp_avg_beat = next(b for b in beats if b.measurement_type == "temperature" and b.observation_type == "average")
        self.assertEqual(temp_avg_beat.value, 53)
        self.assertEqual(temp_avg_beat.period, "daily")
        self.assertTrue(temp_avg_beat.timestamp.startswith("2025-04-17T23:59:59"))

    def test_error_handling(self):
        """Test error handling in get_climate_data."""
        # Arrange
        location_key = "KNYC"
        
        # Test handling of scraper errors
        with patch('observation_scraper.scrapers.cli.CLIScraper.fetch_report', 
                  side_effect=ValueError("Test error")):
            with self.assertRaises(ValueError) as context:
                get_climate_data(location_key)
            
            self.assertIn("Test error", str(context.exception))
        
        # Test handling of parser errors
        with patch('observation_scraper.scrapers.cli.CLIScraper.fetch_report', 
                  return_value=self.sample_html):
            with patch('observation_scraper.parsers.cli.CLIParser.parse_report', 
                      side_effect=ValueError("Parser error")):
                with self.assertRaises(ValueError) as context:
                    get_climate_data(location_key)
                
                self.assertIn("Parser error", str(context.exception))


if __name__ == '__main__':
    unittest.main()
