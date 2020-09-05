import logging
import os

# Create logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s - %(levelname)s:%(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline():
    try:
        logger.info("Starting pipeline...")

        # Get config
        # config_bucket = os.environ["CONFIG_BUCKET"]
        # config_file = os.environ["CONFIG_FILE"]
        # config = configuration.read(config_bucket, config_file)

        # Create Database
        # database.create_bronze_tables(spark, config)
        # database.create_audit_tables(spark, config)

    except Exception:
        logger.exception("Finishing due to error")
        exit(1)

    logger.info("Pipeline Finished")


if __name__ == "__main__":
    run_pipeline()