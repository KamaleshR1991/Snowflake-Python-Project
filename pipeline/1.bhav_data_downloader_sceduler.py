import subprocess
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Setup logger
logger = logging.getLogger("bhav_logger")
logger.setLevel(logging.INFO)

def run_bhav_downloader(**kwargs):
    cmd = ['python', '/opt/airflow/dags/bhav_data_downloader.py']

    try:
        # Capture stdout and stderr
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info("Bhav script output:\n%s", result.stdout)
        if result.stderr:
            logger.warning("Bhav script errors:\n%s", result.stderr)
    except subprocess.CalledProcessError as e:
        logger.error("Bhav script failed with exit code %s", e.returncode)
        logger.error("Stdout:\n%s", e.stdout)
        logger.error("Stderr:\n%s", e.stderr)
        raise  # re-raise to mark the task as FAILED

# DAG definition
with DAG(
    dag_id="bhav_data_downloader_dag",
    start_date=datetime(2025, 10, 26),
    schedule_interval="@daily",
    catchup=False,
    tags=["nse", "bhav"],
) as dag:

    run_bhav_task = PythonOperator(
        task_id="run_bhav_script",
        python_callable=run_bhav_downloader,
    )
