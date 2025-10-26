"""
===========================================================================
Project        : NSE Bhav Copy Downloader
Author         : Kamalesh R.
Date           : 05-Sep-2025
Description    : 
    This script downloads the Bhav copy from the NSE portal (daily EQ bhavcopy)
    and saves it to a specified folder. Supports single date or range.
===========================================================================

"""

import requests
from datetime import datetime, timedelta
import os
import sys
import logging

# ------------------- Configuratio  n -------------------
download_folder = "/opt/airflow/dags/files_saver"
os.makedirs(download_folder, exist_ok=True)  # Create folder if it doesn't exist

# ------------------- Logging Setup -------------------
log_file = os.path.join(download_folder, "bhav_downloader.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)  # Also print to console
    ]
)

# ------------------- Handle Date Parameters -------------------
def parse_date(date_str):
    """Parse date in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        logging.error(f"Invalid date format: {date_str}. Use YYYY-MM-DD (e.g., 2025-10-23).")
        sys.exit(1)

# No argument → today’s date
if len(sys.argv) == 1:
    start_date = end_date = datetime.today()
# One argument → single specific date
elif len(sys.argv) == 2:
    start_date = end_date = parse_date(sys.argv[1])
# Two arguments → date range
elif len(sys.argv) == 3:
    start_date = parse_date(sys.argv[1])
    end_date = parse_date(sys.argv[2])
    if end_date < start_date:
        logging.error("End date cannot be earlier than start date.")
        sys.exit(1)
else:
    logging.error("Too many arguments. Usage: python bhavcopy_downloader.py [start_date] [end_date]")
    sys.exit(1)

# ------------------- Download Loop -------------------
headers = {"User-Agent": "Mozilla/5.0"}

current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime('%d%m%Y')
    formatted_date = current_date.strftime('%d-%b-%Y')
    url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
    file_path = os.path.join(download_folder, f"sec_bhavdata_full_{date_str}.csv")

    logging.info(f"Downloading Bhav Copy for {formatted_date}...")

    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
            logging.info(f"Saved: {file_path}")
        else:
            logging.warning(f"Failed ({response.status_code}) - No file for {formatted_date}")
    except requests.RequestException as e:
        logging.error(f"Error downloading {formatted_date}: {e}")

    current_date += timedelta(days=1)

logging.info("Download process completed.")
