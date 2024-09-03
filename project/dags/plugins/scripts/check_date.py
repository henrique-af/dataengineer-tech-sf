from datetime import datetime
from airflow.operators.python import PythonOperator


def check_date(filename):
    try:
        file_date_str = filename.split('-')[-1]
        file_date = datetime.strptime(file_date_str, '%Y-%m-%d').date()

        if file_date == datetime.now().date():
            return True
    except ValueError:
        return False


def check_file_date_from_xcom(**kwargs):
    ti = kwargs["ti"]
    filename = ti.xcom_pull(task_ids="retrieve_filename")
    return check_date(filename)


check_file_date = PythonOperator(
    task_id="check_file_date",
    python_callable=check_file_date_from_xcom,
    provide_context=True,
)
