from airflow.models import Variable
import pendulum

env = Variable.get("environment")
project_id = f"projeto-{env}"
local_tz = pendulum.timezone("America/Sao_Paulo")
owner = 'Airflow_SF'


if(env == "dev"):
    #gcs vars
    bucket_dag001 =  '.\input\\' #gs://bucket-exemplo-dag-001

    #stage tables
    stage_dag001 = 'sf_stage_dag001'

    #work tables
    work_dag001 = 'sf_work_dag001'

    #target tables
    target_dag001 = 'sf_target_dag001'

    #filename vars
    dag001_filename = 'arquivo-' # arquivo-data do dia em padrão YYYY-MM-DD