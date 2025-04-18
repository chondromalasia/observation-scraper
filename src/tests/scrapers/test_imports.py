import unittest

class TestImports(unittest.TestCase):

    def test_import_cli_scraper(self):
        from observation_scraper.scrapers.cli import CLIScraper
        self.assertIsNotNone(CLIScraper)

if __name__ == "__main__":
    unittest.main()
