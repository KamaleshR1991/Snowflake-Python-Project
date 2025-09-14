#!/usr/bin/env python3
"""
===========================================================================
Project        : CSV Header Remover & Large File Splitter
Author         : Kamalesh R.
Date           : 15-Sep-2025
Description    : 
    - Removes the header line from a large CSV file
    - Splits the headerless CSV into multiple smaller files (~250 MB each)
    - Each output part has no header, ready for chunk-wise processing
    - Efficiently handles large files without loading all data into memory

Usage:
    python data_splitter.py "path/to/your/large_file.csv"
    
Output:
    - A headerless CSV file named <originalname>_no_header.csv
    - Multiple split CSV parts named <originalname>_part1.csv, <originalname>_part2.csv, etc.
===========================================================================
"""

import os
import sys

def remove_header(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        next(infile)  # skip header
        for line in infile:
            outfile.write(line)
    print(f"Header removed. Saved to {output_file}")

def split_file_by_size(input_file, output_prefix, target_size_mb=250):
    target_size_bytes = target_size_mb * 1024 * 1024
    with open(input_file, 'r', encoding='utf-8') as infile:
        part = 1
        outfile = open(f"{output_prefix}{part}.csv", 'w', encoding='utf-8')
        current_size = 0

        for line in infile:
            line_size = len(line.encode('utf-8'))
            if current_size + line_size > target_size_bytes:
                outfile.close()
                part += 1
                outfile = open(f"{output_prefix}{part}.csv", 'w', encoding='utf-8')
                current_size = 0

            outfile.write(line)
            current_size += line_size

        outfile.close()
        print(f"Splitting complete. {part} part(s) created.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python data_splitter.py <input_csv_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    if not os.path.isfile(input_file):
        print(f"File not found: {input_file}")
        sys.exit(1)

    base_name = os.path.splitext(os.path.basename(input_file))[0]
    headerless_file = f"{base_name}_no_header.csv"
    output_prefix = f"{base_name}_part"

    remove_header(input_file, headerless_file)
    split_file_by_size(headerless_file, output_prefix)
