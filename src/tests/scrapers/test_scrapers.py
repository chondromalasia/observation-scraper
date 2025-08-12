import unittest
from unittest.mock import patch, MagicMock

import requests

from observation_scraper.scrapers.cli import CLIScraper
from observation_scraper.config.loader import Config

class TestImports(unittest.TestCase):

    def setUp(self):
        config = Config()
        self.cli_config = config.cli_config["cli"]
        self.scraper = CLIScraper(self.cli_config)
        self.location_key = "KNYC"

    def test_import_cli_scraper(self):
        self.assertIsNotNone(CLIScraper)

    def test_build_url(self):

        url = self.scraper.build_url(self.location_key)


        self.assertTrue(url.startswith("https://forecast.weather.gov/product.php?"))
        
        self.assertIn("site=OKX", url)
        self.assertIn("issuedby=NYC", url)
        self.assertIn("product=CLI", url)
        self.assertIn("format=CI", url)
        self.assertIn("version=1", url)
        self.assertIn("glossary=0", url)

    def test_fetch_report(self):
        """Test fetching CLI report content."""

        mock_response = MagicMock()
        mock_response.text = "Sample CLI Report Content"
        mock_response.status_code = 200
        
        with patch('requests.get', return_value=mock_response) as mock_get:
            report = self.scraper.fetch_report(self.location_key)
            
            mock_get.assert_called_once()
            url_called = mock_get.call_args[0][0]
            self.assertIn("site=OKX", url_called)
            self.assertIn("issuedby=NYC", url_called)
            self.assertEqual(report, "Sample CLI Report Content") 

    def test_fetch_report_connection_error(self):
        """Test connection error handling in fetch_report method."""

        with patch('requests.get', side_effect=requests.ConnectionError("Connection failed")):
            with self.assertRaises(requests.RequestException) as context:
                self.scraper.fetch_report(self.location_key)
            self.assertIn("Error fetching CLI report", str(context.exception))
            self.assertIn("Connection failed", str(context.exception))
    
    def test_fetch_report_http_error(self):
        """Test HTTP error handling in fetch_report method."""

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Client Error")
        with patch('requests.get', return_value=mock_response):
            with self.assertRaises(requests.RequestException) as context:
                self.scraper.fetch_report(self.location_key)
            self.assertIn("Error fetching CLI report", str(context.exception))
            
    def test_fetch_report_empty_response(self):
        """Test empty response handling in fetch_report method."""

        mock_response = MagicMock()
        mock_response.text = "   "
        mock_response.raise_for_status.return_value = None
        with patch('requests.get', return_value=mock_response):
            with self.assertRaises(ValueError) as context:
                self.scraper.fetch_report(self.location_key)
            self.assertIn("Empty response received", str(context.exception))

if __name__ == "__main__":
    unittest.main()
