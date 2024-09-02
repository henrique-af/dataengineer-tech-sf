from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import config.airflow_vars as var
import scripts.check_date as cd

default_args = {
    "owner": var.owner,
    "depends_on_past": False,
    "retries": 0
}

# functions

def get_filename_from_xcom(**kwargs):
    ti = kwargs["ti"]
    filename = ti.xcom_pull(task_ids="unzip_files")
    return filename


with DAG(
    'dag_001_captura_arquivo_landing_zone',
    default_args=default_args,
    description='Essa DAG observa o bucket a cada 10 minutos no horário de 4am - 7am, esse arquivo é entregue pela empresa de logistica',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 9, 1),
    tags=['captura_arquivos', 'logistica', 'unzip'],
    catchup=False
) as dag:

    unzip_files = BashOperator(
        task_id="unzip_files",
        bash_command=f"unzip -o {var.bucket_dag001}/input/*.zip -d gs://{var.bucket_dag001}/unziped/ && ls gs://{var.bucket_dag001}/unziped/ | head -n 1",
        do_xcom_push=True,
    )

    retrieve_filename = PythonOperator(
        task_id="retrieve_filename",
        python_callable=get_filename_from_xcom,
        provide_context=True,
    )

    check_file_date = PythonOperator(
        task_id="check_file_date",
        python_callable=cd.check_file_date,
        provide_context=True
    )

unzip_files >> retrieve_filename >> check_file_date