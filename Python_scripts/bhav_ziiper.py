"""
===========================================================================
Project        : NSE Bhav Copy CSV Filter
Author         : Kamalesh R.
Date           : 05-Sep-2025
Description    : 
    This script processes Bhav Copy ZIP files in a given folder. 
    It performs the following operations:
	1.ZIP the files in the path given
	2.Move the zipped files to NSE,BSE,and other folders
	
Usage          : 
    - Place your zip files in the specified folder.
    - Run the script using Python 3.x.
    - Filtered CSV files will be saved automatically in 'filtered_files' folder.
===========================================================================
"""
import zipfile
import os
import shutil

# Base folder path (where your .zip files are located)
base_path = r"C:\Kamalesh\snow\SOURCE_ZIP"

# Folder for extracted files
unzipped_path = os.path.join(base_path, "unzipped")
os.makedirs(unzipped_path, exist_ok=True)

# Target folders
nse_path = os.path.join(base_path, "nse")
bse_path = os.path.join(base_path, "bse")
others_path = os.path.join(base_path, "others")

# Create target folders if they don't exist
os.makedirs(nse_path, exist_ok=True)
os.makedirs(bse_path, exist_ok=True)
os.makedirs(others_path, exist_ok=True)

# Unzip all files
for filename in os.listdir(base_path):
    if filename.endswith(".zip"):
        zip_path = os.path.join(base_path, filename)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(unzipped_path)
            print(f"Unzipped: {filename}")

# Move files based on name pattern
for file in os.listdir(unzipped_path):
    source_file = os.path.join(unzipped_path, file)

    if "nse.csv" in file.lower():
        shutil.move(source_file, os.path.join(nse_path, file))
        print(f"Moved {file} to NSE folder")
    elif "bse.csv" in file.lower():
        shutil.move(source_file, os.path.join(bse_path, file))
        print(f"Moved {file} to BSE folder")
    elif "nsefo" in file.lower():
        shutil.move(source_file, os.path.join(others_path, file))
        print(f"Moved {file} to Others folder")
    else:
        print(f"Skipped: {file}")
