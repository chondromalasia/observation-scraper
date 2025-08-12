"""CLI (Climatological Report) scraper module."""

from urllib.parse import urlencode
from typing import Dict, Any
import requests

class CLIScraper:
    """
    A scraper for Climate Reports (CLI) from the National Weather Service.
    """
    
    def __init__(self, cli_config: Dict[str, Any]):
        """
        Initialize the CLI scraper with configuration.
        
        Args:
            cli_config: Dictionary containing CLI configuration settings.
        """
        self.cli_config = cli_config

    def build_url(self, location_key: str) -> str:
        """
        Build URL for a specific location's CLI report.
        
        Args:
            location_key: The key of the location in the configuration.
            
        Returns:
            The complete URL for the CLI report.
        """

        base_url = self.cli_config['base_url']
        
        params = self.cli_config['default_params'].copy()
        
        location_params = self.cli_config['locations'][location_key]

        if 'name' in location_params:
            location_params.pop('name')

        params.update(location_params)
        
        url = f"{base_url}?{urlencode(params)}"
        
        return url

    def fetch_report(self, location_key: str) -> str:
        """
        Fetch the CLI report for a specific location.
        
        Args:
            location_key: The key of the location in the configuration.
            
        Returns:
            The content of the CLI report as a string.
            
        Raises:
            KeyError: If the location_key is not found in configuration.
            requests.RequestException: For various request-related errors.
            requests.ConnectionError: If there's a network problem.
            requests.Timeout: If the request times out.
            requests.HTTPError: If the HTTP request returns an unsuccessful status code.
        """
        try:
            url = self.build_url(location_key)
            
            # Make request with timeout
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Check if content is empty
            if not response.text.strip():
                raise ValueError(f"Empty response received from {url}")
                
            return response.text
            
        except requests.RequestException as e:
            # Re-raise with more context
            raise requests.RequestException(
                f"Error fetching CLI report for {location_key}: {str(e)}"
            ) from e
