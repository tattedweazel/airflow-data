from airflow import DAG
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from utils.dag_builder import DagBuilder


############## EDIT THIS PART ####################

event_name = 'dim_user'

############## STOP EDITING HERE ##################

dag_builder = DagBuilder(event_name)

DEFAULT_ARGS = dag_builder.build_default_args()
S3_COPY_SOURCE = 'processed'


dag = DAG(
    dag_id=f'{event_name}_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 8 * * *'
)


redshift_to_s3 = RedshiftToS3Operator(
    task_id=f'{event_name}_redshift_to_s3',
    dag=dag,
    s3_bucket=dag_builder.s3_bucket,
    s3_key=f"{S3_COPY_SOURCE}/{event_name}",
    schema='dw.warehouse',
    table=f'{event_name}',
    redshift_conn_id='redshift',
    unload_options={'FORMAT AS PARQUET', 'ALLOWOVERWRITE'},
    execution_timeout=dag_builder.EXECUTION_TIMEOUT
)


redshift_to_s3
