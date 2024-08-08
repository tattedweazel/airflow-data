from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator
from plugins.operators.stage_copy_operator import StageCopyOperator
from plugins.operators.truncate_table_operator import TruncateTableOperator
from utils.dag_builder import DagBuilder

############## EDIT THIS PART ####################
staging_schema = 'staging'
target_schema = 'warehouse'

events = [
    'ad_event',
    'application_opened',
    'cancel_save_accepted',
    'chat_event',
    'chat_login_complete',
    'chat_shopping_event',
    'chat_signup_complete',
    'chat_signup_event',
    'click_event',
    'community_login_complete',
    'community_signup_complete',
    'community_signup_event',
    'content_block_item_clicked',
    'content_block_link_clicked',
    'content_block_carousel_item_clicked',
    'content_block_view_all_clicked',
    'discord_link_clicked',
    'feed_item_viewed',
    'gate_login_complete',
    'gate_signup_complete',
    'gate_signup_event',
    'hero_event',
    'hero_item_clicked',
    'hero_impression'
]

############## STOP EDITING HERE ##################

dag_name = 'aux_dag_1'
S3_COPY_SOURCE = 'processed'
dag_builder = DagBuilder(dag_name)
DEFAULT_ARGS = dag_builder.build_default_args()
RUN_DATES = dag_builder.get_run_dates()
JOB_FLOW_OVERRIDES = dag_builder.build_overrides()
EVENTS = dag_builder.build_event_jobs(event_names=events, job_path='dags/aux_jobs')
SPARK_STEPS = []

event_columns = {}
truncate_table_tasks = []
s3_copy_tasks = []
stage_tasks = []

dag = DAG(
    dag_id=dag_name,
    default_args=DEFAULT_ARGS,
    schedule_interval='45 8 * * *'
)


def truncate_table_wrapper(target_event):
    task = TruncateTableOperator(
        task_id=f'stage_truncate_{target_event}',
        dag=dag,
        schema=staging_schema,
        table=f'stage_{target_event}',
        redshift_conn_id='redshift'
    )
    return task


def s3_to_redshift_wrapper(run_date, target_event):
    clean_date = f"{run_date['year']}-{run_date['month']}-{run_date['day']}"
    task = S3ToRedshiftOperator(
        task_id=f's3_copy_{target_event}_{clean_date}',
        dag=dag,
        s3_bucket=dag_builder.s3_bucket,
        s3_key=f"{S3_COPY_SOURCE}/{target_event}/year={run_date['year']}/month={run_date['month']}/day={run_date['day']}/",
        table=f'stage_{target_event}',
        schema=f'dw.{staging_schema}',
        redshift_conn_id='redshift',
        copy_options={'FORMAT AS PARQUET'},
        execution_timeout=dag_builder.EXECUTION_TIMEOUT,
        column_list=event_columns[target_event]['original_columns']
    )
    return task


def stage_copy_wrapper(target_event, primary_key, sort_key):
    task = StageCopyOperator(
        task_id=f'stage_copy_{target_event}',
        dag=dag,
        stage_table=f'stage_{target_event}',
        staging_schema=staging_schema,
        target_table=target_event,
        target_schema=target_schema,
        insert_columns=event_columns[target_event]['insert_columns'],
        select_columns=event_columns[target_event]['select_columns'],
        primary_key=primary_key,
        sort_key=sort_key,
        redshift_conn_id='redshift',
        run_dates=RUN_DATES,
        execution_timeout=dag_builder.EXECUTION_TIMEOUT
    )
    return task


def _interstitial():
    print("S3 Copy finished. Moving from Stage.")


for event_name, data in EVENTS.items():
    event_name = event_name
    column_list = data['column_list']
    primary_key = data['primary_key']
    sort_key = 'event_timestamp' if 'sort_key' not in data else data['sort_key']

    dag_builder = DagBuilder(event_name)
    SPARK_STEPS.extend(dag_builder.build_steps(RUN_DATES))
    event_columns[event_name] = dag_builder.build_columns(column_list)
    stage_tasks.append(stage_copy_wrapper(event_name, primary_key, sort_key))

    for run_date in RUN_DATES:
        truncate_table_tasks.append(truncate_table_wrapper(event_name))
        s3_copy_tasks.append(s3_to_redshift_wrapper(run_date, event_name))

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

interstitial_op1 = PythonOperator(
    task_id='interstitial_1',
    python_callable=_interstitial,
    dag=dag
)

interstitial_op2 = PythonOperator(
    task_id='interstitial_2',
    python_callable=_interstitial,
    dag=dag
)

emr_instance_launcher >> emr_step_adder >> emr_instance_monitor >> truncate_table_tasks >> interstitial_op1 >> s3_copy_tasks >> interstitial_op2 >> stage_tasks
