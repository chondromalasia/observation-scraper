import unittest
from unittest.mock import patch, MagicMock
import os
import textwrap
from datetime import datetime

from observation_scraper.parsers.cli import CLIParser

class TestCLIParser(unittest.TestCase):
    """Test cases for CLI report parsing."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample of the relevant part of a CLI report HTML
        self.sample_html = textwrap.dedent('''
            <!DOCTYPE html><html>
            <head><title>National Weather Service</title></head>
            <body>
            <pre class="glossaryProduct">
            978
            CDUS41 KOKX 180617
            CLINYC

            CLIMATE REPORT
            NATIONAL WEATHER SERVICE NEW YORK, NY
            217 AM EDT FRI APR 18 2025

            ...................................

            ...THE CENTRAL PARK NY CLIMATE SUMMARY FOR APRIL 17 2025...

            CLIMATE NORMAL PERIOD 1991 TO 2020
            CLIMATE RECORD PERIOD 1869 TO 2025


            WEATHER ITEM   OBSERVED TIME   RECORD YEAR NORMAL DEPARTURE LAST
                            VALUE   (LST)  VALUE       VALUE  FROM      YEAR
                                                              NORMAL
            ...................................................................
            TEMPERATURE (F)
             YESTERDAY
              MAXIMUM         64    341 PM  96    2002  63      1       62
              MINIMUM         42    542 AM  27    1875  46     -4       50
              AVERAGE         53                        54     -1       56

            PRECIPITATION (IN)
              YESTERDAY        0.00          1.59 1873   0.13  -0.13     0.02
              MONTH TO DATE    2.43                      2.29   0.14     3.35
              SINCE MAR 1      7.95                      6.58   1.37    12.41
              SINCE JAN 1     11.16                     13.41  -2.25    19.74

            SNOWFALL (IN)
              YESTERDAY        0.0           T    1935   0.0    0.0      0.0
              MONTH TO DATE    0.0                       0.4   -0.4      0.0
              SINCE MAR 1      0.0                       5.4   -5.4       T
              SINCE JUL 1     12.9                      29.8  -16.9      7.5
              SNOW DEPTH       0

            DEGREE DAYS
             HEATING
              YESTERDAY       12                        11      1        9
              MONTH TO DATE  270                       240     30      178
              SINCE MAR 1    825                       928   -103      693
              SINCE JUL 1   4086                      4302   -216     3654

             COOLING
              YESTERDAY        0                         0      0        0
              MONTH TO DATE    0                         0      0        8
              SINCE MAR 1      0                         1     -1        8
              SINCE JAN 1      0                         1     -1        8
            ...................................................................


            WIND (MPH)
              HIGHEST WIND SPEED    17   HIGHEST WIND DIRECTION    NW (310)
              HIGHEST GUST SPEED    29   HIGHEST GUST DIRECTION    NW (310)
              AVERAGE WIND SPEED     6.6


            SKY COVER
              AVERAGE SKY COVER 0.2


            WEATHER CONDITIONS
            THE FOLLOWING WEATHER WAS RECORDED YESTERDAY.
              NO SIGNIFICANT WEATHER WAS OBSERVED.


            RELATIVE HUMIDITY (PERCENT)
             HIGHEST    55           300 AM
             LOWEST     15           500 PM
             AVERAGE    35
            </pre>
            </body>
            </html>
        ''')
        
        self.parser = CLIParser()
        self.station_id = "KNYC"
    
    def test_import(self):
        self.assertIsNotNone(CLIParser)

    def test_extract_content(self):
        content = self.parser.extract_report_content(self.sample_html)

        self.assertIn("CLIMATE REPORT", content)
        self.assertIn("CENTRAL PARK NY CLIMATE SUMMARY", content)
        self.assertIn("TEMPERATURE (F)", content)

    def test_extract_report_content_error(self):
        """Test that extracting content from invalid HTML raises the correct error."""
        # Arrange
        invalid_html = "<html><body>No pre element with class glossaryProduct here</body></html>"
        
        # Act and Assert
        with self.assertRaises(ValueError) as context:
            self.parser.extract_report_content(invalid_html)
        
        self.assertIn("Could not find CLI report content", str(context.exception))

    def test_validate_report_type(self):
        """Test validating the report type."""
        # Arrange
        content = self.parser.extract_report_content(self.sample_html)
        
        # Act
        is_valid = self.parser.validate_report_type(content)
        
        # Assert
        self.assertTrue(is_valid)
        
        # Test with invalid content
        invalid_content = "CLIMATE REPORT\nTEMPERATURE (F)\nTODAY"
        self.assertFalse(self.parser.validate_report_type(invalid_content))

    def test_extract_report_content_error(self):
        """Test that extracting content from invalid HTML raises the correct error."""
        # Arrange
        invalid_html = "<html><body>No pre element with class glossaryProduct here</body></html>"
        
        # Act and Assert
        with self.assertRaises(ValueError) as context:
            self.parser.extract_report_content(invalid_html)
        
        self.assertIn("Could not find CLI report content", str(context.exception))

    def test_extract_report_date(self):
        """Test extracting the report date."""
        # Arrange
        content = self.parser.extract_report_content(self.sample_html)
        
        # Act
        date_str, date_obj = self.parser.extract_report_date(content)
        
        # Assert
        self.assertEqual(date_str, "APRIL 17 2025")
        self.assertEqual(date_obj, datetime(2025, 4, 17))

    def test_extract_report_content(self):
        """Test extracting the CLI report content from HTML."""
        # Act
        content = self.parser.extract_report_content(self.sample_html)
        
        # Assert
        self.assertIn("CLIMATE REPORT", content)
        self.assertIn("CENTRAL PARK NY CLIMATE SUMMARY", content)
        self.assertIn("TEMPERATURE (F)", content)

    def test_parse_temperature(self):
        """Test parsing temperature data."""
        # Arrange
        content = self.parser.extract_report_content(self.sample_html)
        
        # Act
        temp_data = self.parser._parse_temperature(content)
        
        # Assert
        self.assertEqual(temp_data["max"], 64)
        self.assertEqual(temp_data["min"], 42)
        self.assertEqual(temp_data["avg"], 53)
        self.assertEqual(temp_data["max_time"], "3:41 PM")
        self.assertEqual(temp_data["min_time"], "5:42 AM")
        self.assertIsNotNone(temp_data["max_datetime"])
        self.assertIsNotNone(temp_data["min_datetime"])

    def test_parse_precipitation(self):
        """Test parsing precipitation data."""
        # Arrange
        content = self.parser.extract_report_content(self.sample_html)
        
        # Act
        precip_data = self.parser._parse_precipitation(content)
        
        # Assert
        self.assertIsNotNone(precip_data)
        self.assertEqual(precip_data["yesterday"], 0.00)

    def test_parse_humidity(self):
        """Test parsing humidity data."""
        # Arrange
        content = self.parser.extract_report_content(self.sample_html)
        
        # Act
        humidity_data = self.parser._parse_humidity(content)
        
        # Assert
        self.assertIsNotNone(humidity_data)
        self.assertEqual(humidity_data["highest"], 55)
        self.assertEqual(humidity_data["lowest"], 15)
        self.assertEqual(humidity_data["average"], 35)

    def test_parse_wind(self):
        """Test parsing wind data."""
        # Arrange
        content = self.parser.extract_report_content(self.sample_html)
        
        # Act
        wind_data = self.parser._parse_wind(content)
        
        # Assert
        self.assertIsNotNone(wind_data)
        self.assertEqual(wind_data["highest_speed"], 17)
        self.assertEqual(wind_data["highest_direction"], "NW")
        self.assertEqual(wind_data["average_speed"], 6.6)

    def test_parse_report(self):
        """Test the complete report parsing process."""
        # Arrange
        content = self.parser.extract_report_content(self.sample_html)
        
        # Act
        cli_data = self.parser.parse_report(content, self.station_id)
        
        # Assert - check required fields
        self.assertEqual(cli_data.station_id, self.station_id)
        self.assertEqual(cli_data.report_date, "APRIL 17 2025")
        self.assertEqual(cli_data.report_datetime, datetime(2025, 4, 17))
        
        # Check required temperature data
        self.assertEqual(cli_data.temperature_max, 64)
        self.assertEqual(cli_data.temperature_min, 42)
        self.assertEqual(cli_data.temperature_avg, 53)
        self.assertEqual(cli_data.temperature_max_time, "3:41 PM")
        self.assertEqual(cli_data.temperature_min_time, "5:42 AM")
        self.assertEqual(cli_data.temperature_max_datetime.hour, 15)
        self.assertEqual(cli_data.temperature_max_datetime.minute, 41)
        self.assertEqual(cli_data.temperature_min_datetime.hour, 5)
        self.assertEqual(cli_data.temperature_min_datetime.minute, 42)
        
        # Check optional fields
        self.assertEqual(cli_data.precipitation_yesterday, 0.00)
        self.assertEqual(cli_data.humidity_highest, 55)
        self.assertEqual(cli_data.humidity_lowest, 15)
        self.assertEqual(cli_data.humidity_avg, 35)
        self.assertEqual(cli_data.wind_highest_speed, 17)
        self.assertEqual(cli_data.wind_highest_direction, "NW")
        self.assertEqual(cli_data.wind_avg_speed, 6.6)

    def test_parse_report_missing_optional_data(self):
        """Test parsing a report with missing optional data."""
        # Arrange - create a minimal report with just temperature data
        minimal_html = textwrap.dedent('''
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
        ''')
        
        content = self.parser.extract_report_content(minimal_html)
        
        # Act
        cli_data = self.parser.parse_report(content, self.station_id)
        
        # Assert - check required fields are present
        self.assertEqual(cli_data.temperature_max, 64)
        self.assertEqual(cli_data.temperature_min, 42)
        self.assertEqual(cli_data.temperature_avg, 53)
        
        # Check optional fields have default values
        self.assertEqual(cli_data.precipitation_yesterday, 0.0)
        self.assertIsNone(cli_data.humidity_avg)
        self.assertIsNone(cli_data.wind_highest_speed)

    def test_parse_report_missing_required_data(self):
        """Test that parsing a report without temperature data raises an error."""
        # Arrange - create a report with no temperature data
        invalid_html = textwrap.dedent('''
            <!DOCTYPE html><html>
            <body>
            <pre class="glossaryProduct">
            CLIMATE REPORT
            NATIONAL WEATHER SERVICE NEW YORK, NY
            217 AM EDT FRI APR 18 2025

            ...THE CENTRAL PARK NY CLIMATE SUMMARY FOR APRIL 17 2025...

            PRECIPITATION (IN)
              YESTERDAY        0.00
            </pre>
            </body>
            </html>
        ''')
        
        content = self.parser.extract_report_content(invalid_html)
        
        # Act & Assert
        with self.assertRaises(ValueError) as context:
            self.parser.parse_report(content, self.station_id)
        
        self.assertIn("Could not extract required temperature data", str(context.exception))


if __name__ == '__main__':
    unittest.main()
