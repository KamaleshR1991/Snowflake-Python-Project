import os
import time
import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
LOG_FILE = "snowflake_write_pandas.log"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds between retries

SF_CONFIG = {
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",
    "warehouse": "COMPUTE_WH",
    "database": "ORDERS_DB",
    "schema": "STAGE"
}

CSV_PATH = r"C:\data\new_orders.csv"
TARGET_TABLE = "STG_ORDERS"

# ------------------------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------------------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Retry decorator
# ------------------------------------------------------------------------------
def retry(func):
    """Retry wrapper for transient Snowflake or network errors."""
    def wrapper(*args, **kwargs):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Attempt {attempt} failed: {e}")
                if attempt < MAX_RETRIES:
                    logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    logger.critical(f"All {MAX_RETRIES} attempts failed. Aborting.")
                    raise
    return wrapper


# ------------------------------------------------------------------------------
# Function: Load via write_pandas
# ------------------------------------------------------------------------------
@retry
def load_with_write_pandas(conn, df):
    logger.info("Starting load via write_pandas()...")
    success, nchunks, nrows, _ = write_pandas(conn, df, TARGET_TABLE)
    logger.info(f"write_pandas() completed → success={success}, chunks={nchunks}, rows={nrows}")
    return success, nrows


# ------------------------------------------------------------------------------
# Main ETL Logic
# ------------------------------------------------------------------------------
def main():
    logger.info("========== Snowflake write_pandas Loader Started ==========")

    try:
        # Step 1: Read CSV
        logger.info(f"Reading file: {CSV_PATH}")
        df = pd.read_csv(CSV_PATH)
        row_count = len(df)
        logger.info(f"Loaded {row_count} rows into DataFrame")

        # Optional: convert date columns if needed
        # df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

        # Step 2: Connect to Snowflake
        conn = snowflake.connector.connect(**SF_CONFIG)
        logger.info("Connected to Snowflake successfully.")

        # Step 3: Load via write_pandas()
        success, nrows = load_with_write_pandas(conn, df)
        if success:
            logger.info(f"✅ Successfully loaded {nrows} rows into {TARGET_TABLE}")
        else:
            logger.warning("⚠️ write_pandas() returned success=False")

    except Exception as e:
        logger.exception(f"Fatal error during ETL: {e}")

    finally:
        try:
            conn.close()
            logger.info("Snowflake connection closed.")
        except Exception:
            pass

    logger.info("========== Snowflake write_pandas Loader Finished ==========")


# ------------------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
