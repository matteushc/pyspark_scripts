import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
	EmrTerminateJobFlowOperator
)
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator

# DAG defaults
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG(
    dag_id='example_emr_dag',
    default_args=default_args,
    description='An example DAG for AWS EMR',
    schedule_interval=None,
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=['example', 'emr'],
) as dag:

    # Task to read JSON from S3
    def read_json_from_s3(bucket_name, key):
        """
        Reads a JSON file from S3 and returns its content as a dictionary.
        """
        s3_hook = S3Hook(aws_conn_id='aws_default')
        file_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
        return json.loads(file_content)

    # Define the S3 bucket and key
    S3_BUCKET_NAME = 'bucket_name'
    S3_KEY = 'scripts_jobs/template.json'
    job_flow_overrides = read_json_from_s3(S3_BUCKET_NAME, S3_KEY)


    @task
    def generate_list():
        list_cluster = [
            {"cluster_name": "cluster_1", "file_name": "s3://bucket_name/script_1.py"},
            {"cluster_name": "cluster_2", "file_name": "s3://bucket_name/script_2.py"}
        ]
        return list_cluster

    @task
    def emit_step(config):
        spark_steps = [{
                'Name': 'Run Spark Job',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        config['file_name']
                    ]
                }
            }]
        return spark_steps


    @task_group(group_id="group_create_emr")
    def create_emr_task_group(config, **kwargs):

        print(kwargs)
        create = EmrCreateJobFlowOperator(
            task_id=f'create_cluster',
            job_flow_overrides=job_flow_overrides,
            aws_conn_id='aws_default',
            retries=1
        )

        add_steps = EmrAddStepsOperator(
            task_id=f'add_steps',
            job_flow_id=create.output,
            steps=emit_step(config),
            aws_conn_id='aws_default',
            retries=0
        )

        watch_step = EmrStepSensor(
            task_id=f'watch_step',
            job_flow_id=create.output,
            step_id="{{ task_instance.xcom_pull(task_ids='group_create_emr.add_steps', map_indexes=task_instance.map_index, key='return_value')[0] }}",
            aws_conn_id='aws_default',
            retries=0
        )

        create >> add_steps >> watch_step

    lista = generate_list()
    
    create_emr = create_emr_task_group.expand(config=lista)


    # Final task after all sensors
    def finalize_process(**kwargs):
        logging.info("All EMR jobs completed. Finalizing process...")

    finalize = PythonOperator(
        task_id='finalize_process',
        python_callable=finalize_process,
        provide_context=True,
        dag=dag,
    )

    create_emr >> finalize
