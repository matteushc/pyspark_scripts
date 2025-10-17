from __future__ import annotations
from datetime import datetime
import os
import json
import textwrap
import logging
import pytz
import time
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.hooks.package_index import PackageIndexHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from hermond.notifications import AWSNotifier
from airflow.operators.dummy import DummyOperator

from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
	EmrTerminateJobFlowOperator
)
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.providers.amazon.aws.hooks.emr import EmrHook


class EmrClusterLifecycleSensor(BaseSensorOperator):

    template_fields = (*BaseSensorOperator.template_fields, "steps")

    def __init__(self, cluster_config, steps, aws_conn_id='aws_default', poll_interval=60, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cluster_config = cluster_config
        self.steps = steps
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval

    def poke(self, context):
        hook = EmrHook(aws_conn_id=self.aws_conn_id)

        ti = context['ti']
        map_idx = ti.map_index
        job_flow_key = f'job_flow_id_{map_idx}'
        # Step 1: Create Cluster
        if not context.get('ti').xcom_pull(task_ids=self.task_id, map_indexes=map_idx, key=job_flow_key):
            response = hook.create_job_flow(self.cluster_config)
            job_flow_id = response['JobFlowId']
            ti.xcom_push(key=job_flow_key, value=job_flow_id)
            self.log.info(f"Cluster criado: {job_flow_id}")
            return False

        job_flow_id = ti.xcom_pull(task_ids=self.task_id, map_indexes=map_idx, key=job_flow_key)

        #Step 2: Adiciona Steps
        steps_added_key = f'steps_added_{map_idx}'
        if not context.get('ti').xcom_pull(task_ids=self.task_id, map_indexes=map_idx, key=steps_added_key):
            hook.add_job_flow_steps(job_flow_id=job_flow_id, steps=self.steps)
            ti.xcom_push(key=steps_added_key, value=True)
            self.log.info(f"Steps adicionados ao cluster {job_flow_id}")
            return False

        # Step 3: Verifica estado do cluster
        cluster_desc = hook.conn.describe_cluster(ClusterId=job_flow_id)
        state = cluster_desc['Cluster']['Status']['State']
        self.log.info(f"Estado atual do cluster {job_flow_id}: {state}")

        if state == 'TERMINATED':
            self.log.info(f"Cluster {job_flow_id} finalizado.")
            return True
        elif state in ['TERMINATING', 'RUNNING', 'WAITING', 'BOOTSTRAPPING']:
            time.sleep(self.poll_interval)
            return False
        else:
            raise Exception(f"Cluster {job_flow_id} em estado inesperado: {state}")


class BaseSparkJob(EmrAddStepsOperator):

    template_fields = (*EmrAddStepsOperator.template_fields, "job_flow_overrides")

    def __init__(self, job_flow_overrides, **kwargs):
        self.job_flow_overrides = job_flow_overrides
        self.job_flow_id = "1"
        kwargs["deferrable"] = True
        super().__init__(**kwargs)

    def execute(self, context: Context) -> list[str]:
        hook = EmrHook(aws_conn_id=self.aws_conn_id)

        response = hook.create_job_flow(self.job_flow_overrides)
        self.job_flow_id = response['JobFlowId']

        return super().execute(context)


@dag(
    dag_id = DAG_NAME.replace(' ', ''),
    dag_display_name = DAG_NAME,
    description = __doc__.partition(".")[0],
    start_date = datetime(2024, 11, 19),
    schedule = "0 9 * * 3",
    catchup = False,
    max_active_runs = 1,
    # DAG default arguments
    default_args = {
        "owner": "Books",
        "depends_on_past": False,
        "email_on_failure": False,
        "on_failure_callback": on_failure_callback
    },
    tags = DAG_TAGS
)
def dag():

    def read_json_from_s3(bucket_name, key):
        """
        Reads a JSON file from S3 and returns its content as a dictionary.
        """
        s3_hook = S3Hook(aws_conn_id='aws_default')
        file_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
        return json.loads(file_content)

    # Define the S3 bucket and key
    S3_BUCKET_NAME = 'bucket'
    S3_KEY = 'config.json'
    job_flow_overrides = read_json_from_s3(S3_BUCKET_NAME, S3_KEY)

    @task
    def get_list_cluster(config_pair: dict):

        return ["cluster1", "cluster1" ]
     
   
    list_of_cluster = get_list_cluster()

    @task
    def emit_step(config):
        return [
            {
                'Name': 'job-spark',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        '--master', 'yarn',
                        '--conf', 'spark.driver.maxResultSize=5g',
                        '--py-files', 's3://bucket/lib.zip',
                        's3://bucket/script.py',
                        '--param', config['param']
                    ]
                }
            }
        ]
    

    @task_group(group_id="group_create_emr")
    def create_emr_task_group(config):
        
        start_emr_process = DummyOperator(task_id='start_process')

        # create = BaseSparkJob(
        #     task_id=f'create_cluster',
        #     job_flow_overrides=job_flow_overrides,
        #     aws_conn_id='aws_default', 
        #     retries=0,
        #     job_flow_id="123",
        #     steps=emit_step(config),
        #     pool='pool'
        #     )

        create = EmrClusterLifecycleSensor(
            task_id='create_cluster',
            cluster_config=job_flow_overrides,
            steps=emit_step(config),
            poke_interval=60,
            pool='pool'
        )

        start_emr_process >> create

    
    create_emr = create_emr_task_group.expand(config=list_of_cluster)


# Entry Point to DAG
if __name__ == '__main__':
    dag().test(mark_success_pattern=".*")
    # dag()
else:
    dag()
