from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime
import os
import pandas as pd
import psycopg2
import plugins.config.airflow_vars as var
import plugins.scripts.unzip_file as unzip
import logging
import plugins.scripts.cleanup_files as clean

# Define paths and variables
input_path = f"{var.bucket_dag001}/input/"
processing_path = f"{var.bucket_dag001}/processing/"
output_path = f"{var.bucket_dag001}/output/"
table_name = f"{var.work_table_dag001}"
plugin_path = f"{var.bucket_plugin_dag001}/scripts/"


# Function to check file date
def check_files(input_path: str):
    today = datetime.now().strftime("%Y-%m-%d")
    for file_name in os.listdir(input_path):
        if today in file_name:
            logging.info(f"File with today's date found: {file_name}")
            return True  # Continue the DAG execution
    logging.info("No files with today's date found. Skipping processing.")
    return False  # Skip the DAG execution


# Function to process CSV file and generate a query
def process_csv(processing_path: str, table_name: str):
    logging.info("Starting to process CSV files.")
    upsert_query = f"""
    INSERT INTO {table_name} (order_id, customer_name, address, city, state, zip_code, country, order_date, delivery_date, status)
    VALUES
    """
    values = []
    for file_name in os.listdir(processing_path):
        if file_name.endswith(".csv"):
            logging.info(f"Processing file: {file_name}")
            df = pd.read_csv(os.path.join(processing_path, file_name))
            for index, row in df.iterrows():
                values.append(
                    f"('{row['OrderID']}', '{row['CustomerName']}', '{row['Address']}', '{row['City']}', '{row['State']}', '{row['ZipCode']}', '{row['Country']}', '{row['OrderDate']}', '{row['DeliveryDate']}', '{row['Status']}')"
                )
    upsert_query += ", ".join(values)
    upsert_query += f"""
    ON CONFLICT (order_id) DO UPDATE SET
        customer_name = EXCLUDED.customer_name,
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        zip_code = EXCLUDED.zip_code,
        country = EXCLUDED.country,
        order_date = EXCLUDED.order_date,
        delivery_date = EXCLUDED.delivery_date,
        status = EXCLUDED.status;
    """
    with open(os.path.join(processing_path, "upsert_queries_dag001.sql"), "w") as file:
        file.write(upsert_query)
    logging.info("CSV processing complete and SQL file generated.")


# Function to update the database
def update_database(processing_path: str, plugin_path: str):
    logging.info("Connecting to the database.")
    conn = psycopg2.connect(
        dbname="postgres",
        user="airflow",
        password="airflow",
        host="172.18.0.2",
        port="5432",
    )
    cursor = conn.cursor()
    with open(
        os.path.join(f"{plugin_path}", "create_table_work_table_dag001.sql"), "r"
    ) as file:
        create_table_query = file.read()  # Create table if not exists
    cursor.execute(create_table_query)
    logging.info("Table created.")
    with open(os.path.join(processing_path, "upsert_queries_dag001.sql"), "r") as file:
        query = file.read()
    cursor.execute(query)
    conn.commit()
    logging.info("Database update complete.")
    cursor.close()
    conn.close()


# Generate a daily report with orders that have been delivered
def generate_report(output_path: str, table_name: str):
    logging.info("Generating report from the database.")
    conn = psycopg2.connect(
        dbname=var.postgres_db,
        user=var.postgres_user,
        password=var.pastgres_password,
        host=var.postgres_host,
    )
    query = f"SELECT * FROM {table_name} WHERE status = 'Entregue';"
    df = pd.read_sql_query(query, conn)
    report_filename = os.path.join(
        output_path, f"report_{datetime.now().strftime('%Y-%m-%d')}.csv"
    )
    df.to_csv(report_filename, index=False)
    conn.close()
    logging.info(f"Report generated: {report_filename}")


# Cleanup function to remove outdated and processed files
def cleanup_files(input_path: str, processing_path: str):
    logging.info("Starting cleanup of files.")
    today = datetime.now().date()
    for path in [input_path, processing_path]:
        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            file_date_str = file_name.split("-")[-1].replace(".csv", "")
            try:
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                if file_date != today:
                    logging.info(f"Removing outdated file: {file_name}")
                    os.remove(file_path)
            except ValueError:
                logging.info(f"Removing processed file: {file_name}")
                os.remove(file_path)
    logging.info("Cleanup complete.")


# Default args for the DAG
default_args = {
    "owner": f"{var.owner}",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

# Define the DAG
with DAG(
    dag_id="dag_001_capture_and_generate_report",
    default_args=default_args,
    description="Essa DAG observa o bucket a cada 1 minuto no horário de 4am - 7am, esse arquivo é entregue pela empresa de logistica",
    schedule_interval="*/1 4-6 * * 1-5",  # Run every minute between 4am and 7am, Monday to Friday
    start_date=datetime(2024, 9, 3),
    catchup=False,
) as dag:

    check_files_task = ShortCircuitOperator(
        task_id="check_files",
        python_callable=check_files,
        op_kwargs={
            "input_path": input_path,
        },
    )

    unzip_task = PythonOperator(
        task_id="unzip_file",
        python_callable=unzip.unzip_file,
        op_kwargs={
            "input_path": input_path,
            "processing_path": processing_path,
        },
    )

    process_task = PythonOperator(
        task_id="process_csv",
        python_callable=process_csv,
        op_kwargs={
            "processing_path": processing_path,
            "table_name": table_name,
        },
    )

    update_task = PythonOperator(
        task_id="update_database",
        python_callable=update_database,
        op_kwargs={
            "processing_path": processing_path,
            "plugin_path": plugin_path,
        },
    )

    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
        op_kwargs={
            "output_path": output_path,
            "table_name": table_name,
        },
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_files",
        python_callable=clean.cleanup_files,
        op_kwargs={
            "input_path": input_path,
            "processing_path": processing_path,
        },
    )

    (
        check_files_task
        >> unzip_task
        >> process_task
        >> update_task
        >> report_task
        >> cleanup_task
    )
