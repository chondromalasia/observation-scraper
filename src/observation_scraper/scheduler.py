import time
import sys
import logging

import schedule

from observation_scraper.cli_operations import get_and_publish_kafka_beats

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    force=True
    )

logger = logging.getLogger(__name__)

def start():

    logger.info("Starting the Scheduler")

    schedule.every().day.at("10:00", "UTC").do(get_and_publish_kafka_beats,
                                               location_key="KNYC",
                                               topic="observations")

    while True:
        schedule.run_pending()
        time.sleep(60)

    logger.info("Scheduler exited for some reason")
