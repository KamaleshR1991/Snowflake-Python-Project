"""
===========================================================================
Project        : NSE Bhav Copy CSV Filter
Author         : Kamalesh R.
Date           : 05-Sep-2025
Description    : 
    This script processes NSE Bhav Copy CSV files in a given folder. 
    It performs the following operations:
        1. Lists all CSV files in the folder.
        2. Checks if the file header matches the expected NSE bhav copy header.
        3. Filters rows where the 'SERIES' column (2nd column) is 'EQ' (case-insensitive).
        4. Saves the filtered data (without header) into a new CSV in the 'filtered_files' subfolder.
Usage          : 
    - Place your CSV files in the specified folder.
    - Run the script using Python 3.x.
    - Filtered CSV files will be saved automatically in 'filtered_files' folder.
===========================================================================
"""

import csv
import glob
import os

# ---------------------------- Configuration ----------------------------
folder_path = r"C:\Users\kamalesh\Downloads\2025-08-07-2025-09-04"
output_folder = os.path.join(folder_path, "filtered_files")
os.makedirs(output_folder, exist_ok=True)

# Expected header (tab-separated for comparison)
expected_header = [
    "SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "LAST",
    "PREVCLOSE", "TOTTRDQTY", "TOTTRDVAL", "TIMESTAMP", "TOTALTRADES", "ISIN"
]

# ---------------------------- List CSV Files ---------------------------
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
print("CSV files found:", csv_files)

# --------------------------- Process Each File -------------------------
for file in csv_files:
    with open(file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        if header == expected_header:
            # Filter rows where column 2 (SERIES) is EQ (case-insensitive)
            filtered_rows = [
                row for row in reader
                if len(row) > 1 and row[1].strip().upper() == "EQ"
            ]
            
            # Output file path
            base_name = os.path.basename(file)
            output_file = os.path.join(output_folder, f"filtered_{base_name}")
            
            # Write filtered data without the header
            with open(output_file, mode='w', newline='', encoding='utf-8') as out_f:
                writer = csv.writer(out_f)
                writer.writerows(filtered_rows)  # no header
                
            print(f"Processed {base_name} -> {output_file}")
        else:
            print(f"Skipped {os.path.basename(file)}: header does not match")
