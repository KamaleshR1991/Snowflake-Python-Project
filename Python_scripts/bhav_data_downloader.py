"""
===========================================================================
Project        : NSE Bhav Copy Downloader
Author         : Kamalesh R.
Date           : 05-Sep-2025
Description    : 
    This script downloads the Bhav copy from NSE portal and saves it
    to a specified folder.
Usage          : 
    - Run the script using Python 3.x.
    - CSV files will be downloaded to the target folder.
===========================================================================
"""

import requests
from datetime import datetime
import os

# ------------------- Configuration -------------------
download_folder = r"C:\Kamalesh\snow\2025-08-07-2025-09-04"
os.makedirs(download_folder, exist_ok=True)  # Create folder if it doesn't exist

# Format date as DDMMYYYY
date_str = datetime.today().strftime('%d%m%Y')

# NSE Bhavcopy URL (daily EQ bhavcopy)
url = f"https://www.nseindia.com/content/historical/EQUITIES/{datetime.today().strftime('%Y')}/{datetime.today().strftime('%b').upper()}/cm{date_str}bhav.csv.zip"

# Headers for HTTP request
headers = {
    "User-Agent": "Mozilla/5.0"
}

# ------------------- Download Bhav Copy -------------------
response = requests.get(url, headers=headers)
if response.status_code == 200:
    file_path = os.path.join(download_folder, f"cm{date_str}bhav.csv.zip")
    with open(file_path, "wb") as f:
        f.write(response.content)
    print(f"Bhav Copy downloaded successfully to {file_path}")
else:
    print("Failed to download Bhav Copy. Check URL or NSE site restrictions.")
