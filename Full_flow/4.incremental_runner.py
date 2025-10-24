import requests
import json
import time
import logging
from datetime import datetime

# --- Setup logging ---
log_file = "dbt_incremental_run.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Also log to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# --- Configuration ---
ACCOUNT_ID = "<your_account_id>"
JOB_ID = "<your_job_id>"
API_KEY = "<your_dbt_cloud_api_key>"

# --- Endpoints ---
trigger_url = f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/"
status_url_template = f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/runs/{{run_id}}"

headers = {
    "Authorization": f"Token {API_KEY}",
    "Content-Type": "application/json"
}

# --- Trigger incremental models ---
payload = {
    "cause": "Trigger incremental models via API",
    "steps_override": ["dbt run --select +incremental"]
}

try:
    response = requests.post(trigger_url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    logging.error(f"Failed to trigger dbt job: {e}")
    exit(1)

run_id = response.json().get('data', {}).get('id')
logging.info(f"Incremental models job triggered! Run ID: {run_id}")

# --- Poll job status ---
poll_interval = 10  # seconds
while True:
    try:
        status_response = requests.get(status_url_template.format(run_id=run_id), headers=headers)
        status_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get run status: {e}")
        break

    run_data = status_response.json().get('data', {})
    status = run_data.get('status_humanized', 'unknown')
    logging.info(f"Current status: {status}")

    if status in ["Success", "Error", "Cancelled"]:
        if status == "Success":
            logging.info("Incremental models job completed successfully!")
        else:
            logging.warning(f"Job finished with status: {status}")
        break

    time.sleep(poll_interval)
