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

    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info("Bhav script output:\n%s", result.stdout)
    if result.stderr:
        logger.warning("Bhav script errors:\n%s", result.stderr)
    if result.returncode != 0:
        raise Exception(f"Bhav downloader failed with exit code {result.returncode}")

def run_file_checker(**kwargs):
    cmd = ['python', '/opt/airflow/dags/file_checker.py']

    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info("File checker output:\n%s", result.stdout)
    if result.stderr:
        logger.warning("File checker errors:\n%s", result.stderr)
    if result.returncode != 0:
        raise Exception(f"File checker failed with exit code {result.returncode}")
        
def run_loader(**kwargs):
    cmd = ['python', '/opt/airflow/dags/loader.py']

    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info("File checker output:\n%s", result.stdout)
    if result.stderr:
        logger.warning("File checker errors:\n%s", result.stderr)
    if result.returncode != 0:
        raise Exception(f"File checker failed with exit code {result.returncode}")


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

    file_checker_task = PythonOperator(
        task_id="run_file_checker",
        python_callable=run_file_checker,
    )
   
    file_loader_task = PythonOperator(
        task_id="run_loader",
        python_callable=run_loader,
    )

    # ------------------- Set dependency -------------------
    run_bhav_task >> file_checker_task  >>  file_loader_task# File checker runs only after downloader succeeds
