from __future__ import annotations
from datetime import datetime
import os
import json
import textwrap
import logging
import pytz
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.hooks.package_index import PackageIndexHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from hermond.notifications import AWSNotifier
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
	EmrTerminateJobFlowOperator
)
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.utils.context import Context
from botocore.config import Config


ENV = Variable.get("ENVIRONMENT").lower()
DAG_TAGS = ["emr"]


def read_json_from_s3(bucket_name, key):
    """
    Reads a JSON file from S3 and returns its content as a dictionary.
    """
    s3_hook = S3Hook(aws_conn_id='aws_default')
    file_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
    return json.loads(file_content)


class CustomEmrCreateJobFlowOperator(EmrCreateJobFlowOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        
        boto3_config = Config(
            retries = {
                'max_attempts': 12,
                'mode': 'standard'
            }
        )
        emr = EmrHook(
            aws_conn_id=self.aws_conn_id,
            emr_conn_id=self.emr_conn_id,
            region_name=self.region_name,
            config=boto3_config 
        )

        self.log.info(
            'Creating JobFlow using aws-conn-id: %s, emr-conn-id: %s',
            self.aws_conn_id, self.emr_conn_id
        )
        response = emr.create_job_flow(self.job_flow_overrides)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('JobFlow creation failed: %s' % response)
        else:
            self.log.info('JobFlow with id %s created', response['JobFlowId'])
            return response['JobFlowId']


def on_failure_callback(context):
    ti = context["task_instance"]

    msg = f"Falha na DAG {ti.dag_id}, Task {ti.task_id}"
    logging.error(f"Failed to send Teams message: {msg}")


@dag(
    dag_id = "test_emr",
    dag_display_name = "Test EMR",
    description = __doc__.partition(".")[0],
    start_date = datetime(2024, 11, 19),
    schedule = "0 9 * * 3" 
    catchup = False,
    max_active_runs = 1,
    # DAG default arguments
    default_args = {
        "owner": "root",
        "depends_on_past": False,
        "email_on_failure": False,
        "on_failure_callback": on_failure_callback
    },
    tags = DAG_TAGS
)
def dag():

    # Define the S3 bucket and key
    S3_BUCKET_NAME = 'basic-bucket'
    S3_KEY = 'scripts_jobs/cluster_large_r7_dag.json'

    lista_template_clusters = {
        "cluster_small_ccf": "scripts_jobs/config_temp/cluster_small_ccf.json",
        "cluster_small_lawsuit": "scripts_jobs/config_temp/cluster_small_lawsuit.json",
        "cluster_small_consultas": "scripts_jobs/config_temp/cluster_small_consultas.json"
    }

    #job_flow_overrides = read_json_from_s3(S3_BUCKET_NAME, S3_KEY)

    @task
    def get_list_of_aggregations():
        
        list_cluster = [
            {"file_name": "s3://basic-bucket/scripts_jobs/test_run.py", "args1": "--demand_id", "args2": "xx1", "cluster_type": "cluster_small_xx1"},
            {"file_name": "s3://basic-bucket/scripts_jobs/test_run.py", "args1": "--demand_id", "args2": "xx2", "cluster_type": "cluster_small_xx2"},
            {"file_name": "s3://basic-bucket/scripts_jobs/test_run.py", "args1": "--demand_id", "args2": "xx3", "cluster_type": "cluster_small_xx3"}
        ]
        return list_cluster

    # Execute Glue Job for metadata generation (Job0)
    list_of_aggregations = get_list_of_aggregations()

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
                        config['file_name'],
                        config['args1'], config['args2']
                    ]
                }
            }]
        return spark_steps


    @task
    def create_template(config):

        run_cluster_flow = config['cluster_type']
        cluster_file = lista_template_clusters[run_cluster_flow]

        job_flow_overrides = read_json_from_s3('basic-bucket', cluster_file)
        return job_flow_overrides


    @task_group(group_id="group_create_emr")
    def create_emr_task_group(config):

        create = CustomEmrCreateJobFlowOperator(
            task_id=f'create_cluster',
            job_flow_overrides=create_template(config),
            aws_conn_id='aws_default',
            retries=0
        )

        add_steps = EmrAddStepsOperator(
            task_id=f'add_steps',
            job_flow_id=create.output,
            steps=emit_step(config),
            aws_conn_id='aws_default',
            retries=3
        )

        watch_step = EmrStepSensor(
            task_id=f'watch_step',
            job_flow_id=create.output,
            step_id="{{ task_instance.xcom_pull(task_ids='group_create_emr.add_steps', map_indexes=task_instance.map_index, key='return_value')[0] }}",
            aws_conn_id='aws_default',
            poke_interval=180,
            timeout=300,
            retries=3,
            deferrable=True
        )

        create >> add_steps >> watch_step
    
    create_emr = create_emr_task_group.expand(config=list_of_aggregations)

    create_emr

# Entry Point to DAG
if __name__ == '__main__':
    dag().test(mark_success_pattern=".*")
    # dag()
else:
    dag()
