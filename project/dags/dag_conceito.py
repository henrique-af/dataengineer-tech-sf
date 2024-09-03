from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartTemplateOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import logging
import plugins.config.airflow_vars as var


# cleanup
def cleanup_gcs_files(bucket_name: str, prefix: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        logging.info(f"Deleting {blob.name}")
        blob.delete()


default_args = {
    "owner": f"{var.owner}",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "dag_conceito",
    default_args=default_args,
    description="Essa dag reproduz o conceito do ambiente apresentado no teste técnico",
    schedule_interval="0 7 * * 1-5",
    start_date=days_ago(2),
    tags=["dag_conceito"],
) as dag:

    # input to processing
    move_gcs_files = GCSToGCSOperator(
        task_id="move_gcs_files",
        source_bucket=var.bucket_dag_conceito,
        source_objects=["input/file.csv"],
        destination_bucket=var.bucket_dag_conceito,
        destination_object="processing/file.csv",
        move_object=True,
    )

    # start dataflow with a template
    start_dataflow = DataflowStartTemplateOperator(
        task_id="start_dataflow_job",
        project=var.gcp_project_id,
        region=var.gcp_region,
        job_name="dataflow-job-conceito",
        template=f"gs://{var.bucket_dag_conceito}/dataflow_templates/template.json",
        parameters={
            "inputFile": f"gs://{var.bucket_dag_conceito}/processing/file.csv",  # Parâmetro de entrada
            "outputFile": f"gs://{var.bucket_dag_conceito}/output/processed.csv",  # Parâmetro de saída
        },
    )

    # load data to gcp table
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=var.bucket_dag_conceito,
        source_objects=["output/processed.csv"],
        destination_project_dataset_table=f"{var.gcp_project_id}.dataset.table_conceito",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
    )

    # clean files that have been processed
    cleanup_files = PythonOperator(
        task_id="cleanup_files",
        python_callable=cleanup_gcs_files,
        op_kwargs={
            "bucket_name": var.bucket_dag_conceito,
            "prefix": "processing/",
        },
    )

    move_gcs_files >> start_dataflow >> load_to_bigquery >> cleanup_files
