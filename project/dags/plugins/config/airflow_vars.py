import pendulum

project_id = f"projeto-dataengineer-sf"
local_tz = pendulum.timezone("America/Sao_Paulo")
owner = "airflow"

# gcs vars
bucket_dag001 = "plugins/bucket/"  # gs://bucket-exemplo-dag-001

# stage tables
stage_dag001 = "sf_stage_dag001"

# work tables
work_dag001 = "sf_work_dag001"

# target tables
target_dag001 = "sf_target_dag001"

# filename vars
dag001_filename = "arquivo-"  # arquivo-data do dia em padrão YYYY-MM-DD
