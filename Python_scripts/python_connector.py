import snowflake.connector
import csv
import logging
import time

# ---------------- Logging Configuration ----------------
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------- Configuration ----------------
SNOWFLAKE_USER = "your_username"
SNOWFLAKE_PASSWORD = "your_password"
SNOWFLAKE_ACCOUNT = "your_account"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "MY_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"
TABLE_NAME = "MY_TABLE"

CSV_FILE = r"C:\Kamalesh\kamalesh\Stock_market\Checker\output.csv"

# ---------------- Connect to Snowflake ----------------
try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()
    logging.info("Connected to Snowflake successfully.")
except Exception as e:
    logging.error(f"Failed to connect to Snowflake: {e}")
    raise

# ---------------- Create Table (if not exists) ----------------
try:
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        SYMBOL STRING,
        SERIES STRING,
        OPEN FLOAT,
        HIGH FLOAT,
        LOW FLOAT,
        CLOSE FLOAT
    )
    """)
    logging.info(f"Table '{TABLE_NAME}' is ready.")
except Exception as e:
    logging.error(f"Failed to create table: {e}")
    raise

# ---------------- Upload CSV Data with Retries ----------------
MAX_RETRIES = 3
with open(CSV_FILE, newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)  # Skip header

    for row_num, row in enumerate(reader, start=1):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                cur.execute(f"""
                    INSERT INTO {TABLE_NAME} (SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, row)
                logging.info(f"Row {row_num} inserted successfully.")
                break  # success, exit retry loop
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed for row {row_num}: {e}")
                time.sleep(2)  # wait before retrying
                if attempt == MAX_RETRIES:
                    logging.error(f"Failed to insert row {row_num} after {MAX_RETRIES} attempts.")

# ---------------- Close Connection ----------------
cur.close()
conn.close()
logging.info("Connection to Snowflake closed.")
print(f"✅ Data uploaded with logging, retries, and error handling.")
