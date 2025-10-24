"""
===========================================================================
Project        : NSE Bhav Copy Downloader (Smart Version)
Author         : Kamalesh R.
Date           : 05-Sep-2025
Description    : 
    Downloads NSE Equity Bhav Copy (.csv.zip) from the NSE portal.
    - If no input is given → downloads for today's date.
    - If one date is given → downloads for that single date.
    - If two dates are given → downloads for that date range.
Usage          : 
    Run the script using Python 3.x.
    Example inputs:
      > python nse_bhavcopy.py
      > python nse_bhavcopy.py 2025-08-07
      > python nse_bhavcopy.py 2025-08-07 2025-09-04
===========================================================================
"""

import requests
from datetime import datetime, timedelta
import os
import sys
import time

# ------------------- Handle Arguments -------------------
args = sys.argv[1:]

if len(args) == 0:
    # No argument → current date
    start_date = end_date = datetime.today()
    print(f"📅 No input provided — downloading today's Bhav Copy ({start_date.strftime('%Y-%m-%d')})")
elif len(args) == 1:
    # Single date
    try:
        start_date = end_date = datetime.strptime(args[0], "%Y-%m-%d")
    except ValueError:
        print("❌ Invalid date format! Use YYYY-MM-DD (e.g. 2025-08-07).")
        sys.exit(1)
else:
    # Date range
    try:
        start_date = datetime.strptime(args[0], "%Y-%m-%d")
        end_date = datetime.strptime(args[1], "%Y-%m-%d")
    except ValueError:
        print("❌ Invalid date format! Use YYYY-MM-DD YYYY-MM-DD (e.g. 2025-08-07 2025-09-04).")
        sys.exit(1)

# ------------------- Configuration -------------------
download_folder = rf"C:\Kamalesh\snow\{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}"
os.makedirs(download_folder, exist_ok=True)

base_url = "https://www.nseindia.com/content/historical/EQUITIES/{year}/{month}/cm{date}bhav.csv.zip"

# ------------------- Setup Session -------------------
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/"
})

# Initialize cookies
session.get("https://www.nseindia.com", timeout=10)

# ------------------- Download Logic -------------------
current_date = start_date
success_count = 0
fail_count = 0

while current_date <= end_date:
    # Skip weekends
    if current_date.weekday() >= 5:  # 5=Sat, 6=Sun
        current_date += timedelta(days=1)
        continue

    date_str = current_date.strftime("%d%m%Y")
    year_str = current_date.strftime("%Y")
    month_str = current_date.strftime("%b").upper()
    url = base_url.format(year=year_str, month=month_str, date=date_str)

    file_name = f"cm{date_str}bhav.csv.zip"
    file_path = os.path.join(download_folder, file_name)

    if os.path.exists(file_path):
        print(f"🟡 Already exists: {file_name}")
        current_date += timedelta(days=1)
        continue

    try:
        response = session.get(url, timeout=15)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"✅ Downloaded: {file_name}")
            success_count += 1
        elif response.status_code == 404:
            print(f"❌ Not available: {file_name} (holiday or not uploaded yet)")
            fail_count += 1
        else:
            print(f"⚠️ Failed ({response.status_code}): {file_name}")
            fail_count += 1
    except requests.exceptions.RequestException as e:
        print(f"🚫 Error downloading {file_name}: {e}")
        fail_count += 1

    time.sleep(1.5)
    current_date += timedelta(days=1)

# ------------------- Summary -------------------
print("\n================ Download Summary ================")
print(f"Start Date       : {start_date.strftime('%d-%b-%Y')}")
print(f"End Date         : {end_date.strftime('%d-%b-%Y')}")
print(f"Files Downloaded : {success_count}")
print(f"Files Skipped    : {fail_count}")
print(f"Saved To         : {download_folder}")
print("==================================================")
