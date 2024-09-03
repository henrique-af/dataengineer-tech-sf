import pytz

project_id = f"projeto-dataengineer-sf"
local_tz = pytz.timezone("America/Sao_Paulo")
owner = "airflow"

# gcs vars
bucket_dag001 = "dags/plugins/bucket" 
bucket_plugin_dag001 = f"dags/plugins"
bucket_dag_conceito = "gs://bucket-exemplo-dag-conceito"

# work tables
work_table_dag001 = "public.work_table_dag001"

# postgres connection
postgres_db = "postgres"
postgres_user = "airflow" # colocar em uma váriavel de ambiente dentro do airflow para segurança
pastgres_password = "airflow" # colocar em uma váriavel de ambiente dentro do airflow para segurança
postgres_host = "172.18.0.2"
