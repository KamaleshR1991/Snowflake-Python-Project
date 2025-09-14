#!/usr/bin/env python3
"""
===========================================================================
Project        : Data Quality Analyzer for Large Datasets
Author         : Kamalesh R 
Date           : 15-Sep-2025
Description    : 
    - Loads large CSV or Excel datasets in chunks
    - Detects and reports:
        - Missing values
        - Duplicate rows
        - Constant columns (only one unique value)
        - High cardinality columns (unique values > 50% of total rows)
        - Columns with mixed data types
    - Supports .csv, .xls, .xlsx file formats
    - Handles large files efficiently via chunking

Usage:
    python data_cleaner.py "path/to/your/file.csv" > data.log 2>&1
===========================================================================    
"""

import pandas as pd
import sys
import os
from collections import defaultdict

def load_file(file_path, chunk_size=10000000):
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == '.csv':
            return pd.read_csv(file_path, chunksize=chunk_size)
        elif ext in ['.xls', '.xlsx']:
            df = pd.read_excel(file_path)
            return [df]  # wrap in list for uniform processing
        else:
            print(f"Unsupported file format: {ext}")
            sys.exit(1)
    except Exception as e:
        print(f"Failed to load file: {e}")
        sys.exit(1)

def data_quality_report(chunks):
    total_rows = 0
    total_duplicates = 0
    unique_counts = defaultdict(set)
    column_types = {}
    all_columns = None
    constant_cols_candidates = None
    mixed_type_columns = set()

    for i, chunk in enumerate(chunks):
        print(f"\n--- Processing Chunk {i + 1} ---")
        total_rows += len(chunk)

        # Capture column names
        if all_columns is None:
            all_columns = chunk.columns.tolist()
            constant_cols_candidates = set(all_columns)

        print(f"Chunk shape: {chunk.shape}")

        # Missing values
        print("\nMissing Values:")
        missing = chunk.isnull().sum()
        print(missing[missing > 0])

        # Duplicates
        dup_count = chunk.duplicated().sum()
        total_duplicates += dup_count
        print(f"Duplicate rows in chunk: {dup_count}")

        # Constant columns
        for col in list(constant_cols_candidates):
            if chunk[col].nunique(dropna=False) > 1:
                constant_cols_candidates.remove(col)

        # High cardinality - collect unique values
        for col in chunk.columns:
            if chunk[col].dtype == object or pd.api.types.is_string_dtype(chunk[col]):
                unique_counts[col].update(chunk[col].dropna().unique())
            else:
                try:
                    unique_counts[col].update(chunk[col].dropna().astype(str).unique())
                except:
                    pass

        # Data types
        for col in chunk.columns:
            current_type = chunk[col].map(type).nunique()
            if current_type > 1:
                mixed_type_columns.add(col)

        # Track data types from first chunk only
        if i == 0:
            print("\nColumn Data Types:")
            print(chunk.dtypes)
            column_types = chunk.dtypes.to_dict()

    print("\n=== Final Summary ===")
    print(f"Total rows processed: {total_rows}")
    print(f"Total duplicate rows: {total_duplicates}")

    print("\n=== Constant Columns ===")
    if constant_cols_candidates:
        print("Columns with only one unique value:", list(constant_cols_candidates))
    else:
        print("No constant columns found.")

    print("\n=== High Cardinality Columns (unique values > 50% of rows) ===")
    high_card_cols = [col for col, values in unique_counts.items() if len(values) > 0.5 * total_rows]
    if high_card_cols:
        print("High cardinality columns:", high_card_cols)
    else:
        print("No high cardinality columns found.")

    print("\n=== Columns with Mixed Data Types ===")
    if mixed_type_columns:
        print("Columns with mixed data types across chunks:", list(mixed_type_columns))
    else:
        print("No mixed data type columns found.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python data_cleaner.py <path_to_dataset>")
        sys.exit(1)

    file_path = sys.argv[1]
    chunks = load_file(file_path)
    data_quality_report(chunks)
