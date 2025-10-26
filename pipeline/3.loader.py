import os
import shutil
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ------------------ Snowflake Connection ------------------
conn = snowflake.connector.connect(
    user="KAMALESHR1911",
    password="************",
    account="************",
    warehouse="KAMALESH",
    database="CMDB_DB",
    schema="CMDB_SCHEMA;"
)
cur = conn.cursor()
cur.execute("USE SCHEMA CMDB_DB.CMDB_SCHEMA")

# ------------------ File Paths ------------------
source_folder = "/opt/airflow/dags/files_saver/filtered_files"
loaded_folder = "/opt/airflow/dags/files_saver/filtered_files/Loaded"
os.makedirs(loaded_folder, exist_ok=True)

# ------------------ Loop through files ------------------
for file_name in os.listdir(source_folder):
    if file_name.endswith(".csv"):
        file_path = os.path.join(source_folder, file_name)
        
        # Read CSV into Pandas DataFrame
        df = pd.read_csv(file_path)
        
        # Convert DATE1 column to datetime
        if 'DATE1' in df.columns:
            df['DATE1'] = pd.to_datetime(df['DATE1'], errors='coerce')
            df['DATE1'] = df['DATE1'].dt.date
        # Load DataFrame into Snowflake
        success, nchunks, nrows, _ = write_pandas(conn, df, 'STG_NSE_BHAV_COPY')
        print(f"{nrows} rows inserted from {file_name}")
        
        # Move the loaded file to 'Loaded' folder
        shutil.move(file_path, os.path.join(loaded_folder, file_name))
        print(f"{file_name} moved to Loaded folder")

# ------------------ Close Connection ------------------
cur.close()
conn.close()
print("All files loaded and moved successfully!")
