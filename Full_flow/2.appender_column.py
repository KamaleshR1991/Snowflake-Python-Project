import csv
import glob
import os
import logging
from datetime import datetime

# ---------------------------- Logging Configuration ----------------------------
logging.basicConfig(
    filename='etl_process.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------------- Configuration ----------------------------
folder_path = r"C:\Kamalesh\kamalesh\Stock_market\Checker"
output_folder = os.path.join(folder_path, "filtered_files")
os.makedirs(output_folder, exist_ok=True)

# Expected header (standard NSE columns)
expected_header = [
    "SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "LAST",
    "PREVCLOSE", "TOTTRDQTY", "TOTTRDVAL", "TIMESTAMP", "TOTALTRADES", "ISIN"
]

# ---------------------------- List CSV Files ----------------------------
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

if not csv_files:
    logging.warning(f"No CSV files found in {folder_path}")
    print(f"⚠️ No CSV files found in {folder_path}")

# ---------------------------- Process Each File ----------------------------
for file in csv_files:
    try:
        with open(file, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            original_header = next(reader)
            col_index_map = {col: idx for idx, col in enumerate(original_header)}

            # ------------------- Data Quality & Validation -------------------
            missing_columns = [col for col in expected_header if col not in col_index_map]
            if missing_columns:
                logging.warning(f"Missing columns in {os.path.basename(file)}: {', '.join(missing_columns)}")
                print(f"⚠️ Missing columns in {os.path.basename(file)}: {', '.join(missing_columns)}")

            # ------------------- Data Transformation -------------------
            adjusted_rows = []
            for row in reader:
                new_row = []
                for col in expected_header:
                    if col in col_index_map:
                        new_row.append(row[col_index_map[col]])
                    else:
                        new_row.append("")  # add blank if missing
                adjusted_rows.append(new_row)

            # Filter rows where SERIES = EQ
            filtered_rows = [row for row in adjusted_rows if len(row) > 1 and row[1].strip().upper() == "EQ"]

            # Output file path
            base_name = os.path.basename(file)
            output_file = os.path.join(output_folder, f"filtered_{base_name}")

            # ------------------- Data Ingestion / Write -------------------
            with open(output_file, mode='w', newline='', encoding='utf-8') as out_f:
                writer = csv.writer(out_f)
                writer.writerow(expected_header)  # Include full header
                writer.writerows(filtered_rows)

            logging.info(f"Processed {base_name} → {output_file} | Rows inserted: {len(filtered_rows)}")
            print(f"✅ Processed: {base_name} → {output_file}")

    except Exception as e:
        logging.error(f"Failed to process {file}: {e}")
        print(f"❌ Error processing {file}: {e}")
