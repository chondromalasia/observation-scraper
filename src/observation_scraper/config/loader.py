from pathlib import Path
from typing import Dict, Any

import yaml

class Config:
    """
    Configuration class for observation scrapers

    Handles loading and accessing configuration data for the observation scrapers.
    """

    def __init__(self):
        self.config_dir = Path(__file__).parent

        self.cli_config = self.load_yaml(self.config_dir / 'cli.yaml')
        self.kafka_config = self.load_yaml(self.config_dir / 'kafka.yaml')

    def load_yaml(self, file_path: str) -> Dict[str, Any]:
        """Load YAML file."""
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)

