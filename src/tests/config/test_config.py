import unittest

from observation_scraper.config.loader import Config

class TestConfig(unittest.TestCase):
    
    def setUp(self):
        self.c = Config()

    def test_config_import(self):
        self.assertIsNotNone(Config)
        self.assertIsInstance(self.c, Config)

    def test_config_dir(self):
        self.assertTrue(self.c.config_dir.exists())
        self.assertTrue(self.c.config_dir.is_dir())

    def test_load_yaml(self):

        cli_yaml_path = self.c.config_dir / "cli.yaml"
        self.assertTrue(cli_yaml_path.exists)

        yaml_data = self.c.load_yaml(cli_yaml_path)

        self.assertIsInstance(yaml_data, dict)
        self.assertIn('cli', yaml_data)
        self.assertIn('base_url', yaml_data['cli'])
        self.assertIn('default_params', yaml_data['cli'])
        self.assertIn('locations', yaml_data['cli'])
        self.assertIn('KNYC', yaml_data['cli']['locations'])

    def test_cli_config_loaded(self):
        """Test that the cli_config is automatically lodaded during initialization."""
        self.assertIsNotNone(self.c.cli_config)
        self.assertIsInstance(self.c.cli_config, dict)

        self.assertIn('cli', self.c.cli_config)


if __name__ == "__main__":
    unittest.main()
