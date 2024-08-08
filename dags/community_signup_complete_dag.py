from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from plugins.operators.stage_copy_operator import StageCopyOperator
from plugins.operators.truncate_table_operator import TruncateTableOperator
from utils.dag_builder import DagBuilder


############## EDIT THIS PART ####################

event_name = 'community_signup_complete'
column_list = [
    "message_id",
    "anonymous_id",
    "user_id",
    "event_timestamp",
    "option_selected",
    "platform"
]
primary_key ='message_id'
sort_key = 'event_timestamp'
staging_schema = 'staging'
target_schema = 'warehouse'

############## STOP EDITING HERE ##################


dag_builder = DagBuilder(event_name)

COLUMNS = dag_builder.build_columns(column_list)
DEFAULT_ARGS = dag_builder.build_default_args()
SPARK_STEPS = dag_builder.build_steps()
JOB_FLOW_OVERRIDES = dag_builder.build_overrides()
RUN_DATES = dag_builder.get_run_dates()
S3_COPY_SOURCE = 'processed'


dag = DAG(
    dag_id=f'{event_name}_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval=None
)

emr_instance_launcher = EmrCreateJobFlowOperator(
    task_id='emr_instance_launcher',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    dag=dag
)

emr_step_adder = EmrAddStepsOperator(
    task_id='emr_step_adder',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='emr_instance_launcher', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
    dag=dag
)

emr_instance_monitor = EmrStepSensor(
    task_id='emr_instance_monitor',
    job_flow_id="{{ task_instance.xcom_pull('emr_instance_launcher', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='emr_step_adder', key='return_value')[-1] }}",
    aws_conn_id='aws_default',
    dag=dag
)

stage_truncate = TruncateTableOperator(
    task_id=f'stage_truncate_{event_name}',
    dag=dag,
    schema=staging_schema,
    table=f'stage_{event_name}',
    redshift_conn_id='redshift'
)

def copy_wrapper(run_date):
    clean_date = f"{run_date['year']}-{run_date['month']}-{run_date['day']}"
    task = S3ToRedshiftOperator(
        task_id=f's3_copy_{event_name}_{clean_date}',
        dag=dag,
        s3_bucket=dag_builder.s3_bucket,
        s3_key=f"{S3_COPY_SOURCE}/{event_name}/year={run_date['year']}/month={run_date['month']}/day={run_date['day']}/",
        table=f'stage_{event_name}',
        schema=f'dw.{staging_schema}',
        redshift_conn_id='redshift',
        copy_options={'FORMAT AS PARQUET'},
        execution_timeout=dag_builder.EXECUTION_TIMEOUT,
        column_list=column_list
    )
    return task


copy_tasks = []
for run_date in RUN_DATES:
    copy_tasks.append(copy_wrapper(run_date))


stage_copy = StageCopyOperator(
    task_id=f'stage_copy_{event_name}',
    dag=dag,
    stage_table=f'stage_{event_name}',
    staging_schema=staging_schema,
    target_table=event_name,
    target_schema=target_schema,
    insert_columns=COLUMNS['insert_columns'],
    select_columns=COLUMNS['select_columns'],
    primary_key=primary_key,
    sort_key=sort_key,
    redshift_conn_id='redshift',
    run_dates=RUN_DATES,
    execution_timeout=dag_builder.EXECUTION_TIMEOUT
)


emr_instance_launcher >> emr_step_adder >> emr_instance_monitor >> stage_truncate >> copy_tasks >> stage_copy
