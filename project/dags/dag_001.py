import os
import zipfile
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import airflow_vars as var

default_args = {
    "owner": var.owner,
    "depends_on_past": False,
    "retries": 0,
}


def unzip_files(**kwargs):
    input_dir = "plugins/bucket/input/"
    output_dir = "plugins/bucket/processing/"

    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"NO DIR FOUND: {input_dir}")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    filenames = []

    for filename in os.listdir(input_dir):
        if filename.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(input_dir, filename), "r") as zip_ref:
                zip_ref.extractall(output_dir)
                filenames.append(filename)

    if filenames:
        kwargs["ti"].xcom_push(key="filename", value=filenames[0])
    else:
        raise ValueError("NO ZIP FILE FOUND")


def get_filename_from_xcom(**kwargs):
    ti = kwargs["ti"]
    filename = ti.xcom_pull(task_ids="unzip_files", key="filename")
    if not filename:
        raise ValueError("NO FILENAME FOUND")
    return filename


def check_date(filename):
    try:
        file_date_str = filename.split("-")[-1]
        file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
        return file_date == datetime.now().date()
    except ValueError:
        return False


def check_file_date_from_xcom(**kwargs):
    ti = kwargs["ti"]
    filename = ti.xcom_pull(task_ids="retrieve_filename")
    return check_date(filename)


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
        python_callable=unzip_files,
        provide_context=True,
    )

    retrieve_filename = PythonOperator(
        task_id="retrieve_filename",
        python_callable=get_filename_from_xcom,
        provide_context=True,
    )

    check_file_date = PythonOperator(
        task_id="check_file_date",
        python_callable=check_file_date_from_xcom,
        provide_context=True,
    )

    unzip_files_task >> retrieve_filename >> check_file_date
