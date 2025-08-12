import unittest
from datetime import datetime

from observation_scraper.models.climate_data import CLIData, KafkaBeat

class TestCLIDataModels(unittest.TestCase):

    def test_imports(self):
       self.assertIsNotNone(CLIData) 
       self.assertIsNotNone(KafkaBeat)

    def test_cli_data_creation(self):
        """Test creating a CLIData instance."""
        # Arrange
        report_date = "APRIL 17 2025"
        report_datetime = datetime(2025, 4, 17)
        station_id = "KNYC"
        
        # Act
        data = CLIData(
            report_date=report_date,
            report_datetime=report_datetime,
            station_id=station_id,
            temperature_max=64,
            temperature_min=42,
            temperature_avg=53,
            temperature_max_time="3:41 PM",
            temperature_min_time="5:42 AM",
            temperature_max_datetime=datetime(2025, 4, 17, 15, 41),
            temperature_min_datetime=datetime(2025, 4, 17, 5, 42),
            precipitation_yesterday=0.0,
            humidity_highest=55,
            humidity_lowest=15,
            humidity_avg=35,
            wind_highest_speed=17,
            wind_highest_direction="NW",
            wind_avg_speed=6.6
        )
        
        # Assert
        self.assertEqual(data.report_date, report_date)
        self.assertEqual(data.report_datetime, report_datetime)
        self.assertEqual(data.station_id, station_id)
        self.assertEqual(data.temperature_max, 64)
        self.assertEqual(data.temperature_min, 42)
        self.assertEqual(data.temperature_max_time, "3:41 PM")
        self.assertEqual(data.precipitation_yesterday, 0.0)
        self.assertEqual(data.humidity_avg, 35)
        self.assertEqual(data.wind_highest_speed, 17)

    def test_cli_data_validation(self):
        """Test validation in the CLIData model."""
        # Test invalid temperature (too high)
        with self.assertRaises(ValueError):
            CLIData(
                station_id="KNYC",
                temperature_max=200  # Too high
            )
        
        # Test invalid humidity (over 100%)
        with self.assertRaises(ValueError):
            CLIData(
                station_id="KNYC",
                humidity_avg=120  # Over 100%
            )
        
        # Test invalid precipitation (negative)
        with self.assertRaises(ValueError):
            CLIData(
                station_id="KNYC",
                precipitation_yesterday=-1.0  # Negative
            )

class TestKafkaBeatModel(unittest.TestCase):
    """Test cases for the KafkaBeat model."""
    
    def test_instantaneous_beat(self):
        """Test creating a KafkaBeat for an instantaneous measurement."""
        # Arrange & Act
        beat = KafkaBeat(
            measurement_type="temperature",
            value=64,
            unit="F",
            observation_type="max",
            timestamp="2025-04-17T15:41:00Z",
            station_id="KNYC",
            service="CLI"
        )
        
        # Assert
        self.assertEqual(beat.measurement_type, "temperature")
        self.assertEqual(beat.value, 64)
        self.assertEqual(beat.unit, "F")
        self.assertEqual(beat.observation_type, "max")
        self.assertEqual(beat.timestamp, "2025-04-17T15:41:00Z")
        self.assertEqual(beat.station_id, "KNYC")
        self.assertEqual(beat.service, "CLI")  # Default value
        self.assertIsNone(beat.period)  # No period for instantaneous
        
    def test_average_beat(self):
        """Test creating a KafkaBeat for an average measurement."""
        # Arrange & Act
        beat = KafkaBeat(
            measurement_type="temperature",
            value=53,
            unit="F",
            observation_type="average",
            period="daily",
            timestamp="2025-04-17T23:59:59Z",
            station_id="KNYC",
            service="CLI"
        )
        
        # Assert
        self.assertEqual(beat.measurement_type, "temperature")
        self.assertEqual(beat.value, 53)
        self.assertEqual(beat.observation_type, "average")
        self.assertEqual(beat.period, "daily")
        self.assertEqual(beat.timestamp, "2025-04-17T23:59:59Z")


if __name__ == '__main__':
    unittest.main()

