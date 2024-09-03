import sys
import os
import logging


sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from plugins.scripts import unzip_files, check_date
import plugins.config.airflow_vars as var

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": var.owner,
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    "dag_001_captura_arquivo_landing_zone",
    default_args=default_args,
    description="Essa DAG observa o bucket a cada 10 minutos no horário de 4am - 7am, esse arquivo é entregue pela empresa de logistica",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 9, 1),
    tags=["captura_arquivos", "logistica", "unzip"],
    catchup=False,
) as dag:

    unzip_files_task = PythonOperator(
        task_id="unzip_files",
        python_callable=unzip_files.unzip_files,
        op_kwargs={'bucket_dag001': var.bucket_dag001}
    )

    retrieve_filename = PythonOperator(
        task_id="retrieve_filename",
        python_callable=unzip_files.get_filename_from_xcom
    )

    check_file_date = PythonOperator(
        task_id="check_file_date",
        python_callable=check_date.check_file_date
    )

    unzip_files_task >> retrieve_filename >> check_file_date
