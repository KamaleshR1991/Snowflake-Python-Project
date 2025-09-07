"""
===========================================================================
Project        : NSE/BSE Bhav Copy CSV Filter & Split
Author         : Kamalesh R.
Date           : 05-Sep-2025
Description    : 
    - For NSE: Filters EQ rows from CSVs
    - For BSE: Adds date from filename as last column
    - Removes headers
    - Splits output into ~100 MB chunks
    - Compresses each chunk to GZIP
    - Accepts folder path and source type (NSE/BSE) as parameters

Usage:
    python bhav_filter_split.py "C:\\path\\to\\folder" NSE
    python bhav_filter_split.py "C:\\path\\to\\folder" BSE
===========================================================================
"""

import csv
import glob
import os
import gzip
import sys
from datetime import datetime

# ---------------------------- Configuration ----------------------------
if len(sys.argv) != 3:
    print("❌ Usage: python bhav_filter_split.py <folder_path> <NSE|BSE>")
    sys.exit(1)

folder_path = sys.argv[1]
source_type = sys.argv[2].strip().upper()

if source_type not in ["NSE", "BSE"]:
    print("❌ Source type must be either 'NSE' or 'BSE'")
    sys.exit(1)

if not os.path.isdir(folder_path):
    print(f"❌ Invalid folder: {folder_path}")
    sys.exit(1)

output_folder = os.path.join(folder_path, "compressed_files")
os.makedirs(output_folder, exist_ok=True)

MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024

# --------------------------- Expected Headers --------------------------
expected_header_nse = [
    "SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "LAST",
    "PREVCLOSE", "TOTTRDQTY", "TOTTRDVAL", "TIMESTAMP", "TOTALTRADES", "ISIN"
]

expected_header_bse = [
    "SC_CODE", "SC_NAME", "SC_GROUP", "SC_TYPE", "OPEN", "HIGH", "LOW",
    "CLOSE", "LAST", "PREVCLOSE", "NO_TRADES", "NO_OF_SHRS", "NET_TURNOV", "TDCLOINDI"
]

# ---------------------------- Helper Functions ----------------------------
def get_gzip_writer(output_path):
    gzip_file = gzip.open(output_path, mode='wt', newline='', encoding='utf-8')
    return gzip_file, csv.writer(gzip_file)

def extract_date_from_filename(file_name):
    try:
        date_part = file_name.split('_')[0]
        date_obj = datetime.strptime(date_part, "%Y%m%d")
        return date_obj.strftime("%d-%b-%Y").upper()
    except Exception as e:
        print(f"⚠️ Failed to extract date from filename '{file_name}': {e}")
        return ""

# --------------------------- Process Files ----------------------------
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
print("📄 CSV files found:", len(csv_files))

file_index = 1
current_file_size = 0
gzip_file = None
writer = None

for file in csv_files:
    base_file_name = os.path.basename(file)
    with open(file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader, None)

        if source_type == "NSE" and header != expected_header_nse:
            print(f"⚠️ Skipped {base_file_name}: header does not match NSE format")
            continue
        elif source_type == "BSE" and header != expected_header_bse:
            print(f"⚠️ Skipped {base_file_name}: header does not match BSE format")
            continue

        file_date = extract_date_from_filename(base_file_name) if source_type == "BSE" else ""

        for row in reader:
            # NSE: Filter rows where SERIES == 'EQ'
            if source_type == "NSE":
                if len(row) < 2 or row[1].strip().upper() != "EQ":
                    continue

            # BSE: Add date as new last column
            if source_type == "BSE":
                row.append(file_date)

            # Lazy init of gzip file and writer
            if gzip_file is None or current_file_size > MAX_FILE_SIZE_BYTES:
                if gzip_file:
                    gzip_file.close()
                part_name = f"{source_type.lower()}_part_{file_index}.csv.gz"
                output_path = os.path.join(output_folder, part_name)
                gzip_file, writer = get_gzip_writer(output_path)
                current_file_size = 0
                file_index += 1
                print(f"📝 Creating new file: {part_name}")

            row_size = sum(len(cell.encode('utf-8')) for cell in row) + len(row)
            writer.writerow(row)
            current_file_size += row_size

# Close last file
if gzip_file:
    gzip_file.close()

print("✅ All files processed and compressed in chunks.")
