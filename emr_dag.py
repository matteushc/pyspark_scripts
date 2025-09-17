from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='example_emr_dag',
    default_args=default_args,
    description='An example DAG for AWS EMR',
    schedule_interval=None,
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=['example', 'emr'],
) as dag:

    # EMR cluster configuration
    JOB_FLOW_OVERRIDES = {
        "Name": "emr-default",
        "ReleaseLabel": "emr-7.8.0",
        "LogUri": "s3://bucket/logs-emr",
        "VisibleToAllUsers": "true",
        "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
        "EbsRootVolumeSize": 50,
        "Applications": [
            { "Name": "Hadoop" },
            { "Name": "Spark" },
            { "Name": "Hive" }
        ],
        "Configurations": [
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.dynamicAllocation.enable": "true",
                    "spark.dynamicAllocation.minExecutors": "1",
                    "spark.shuffle.service.enabled": "true",
                    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.broadcastTimeout": "600",
                    "spark.shuffle.io.connectionTimeout": "600s",
                    "spark.network.timeout": "600s"
                }
            },
            {
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            {
                "Classification" : "spark-env",
                "Configurations" : [
                    {
                        "Classification" : "export",
                        "Properties" : {
                            "PYSPARK_PYTHON" : "/usr/bin/python3"
                        }
                    }
                ]
            }
        ],
        "Instances": {
            "KeepJobFlowAliveWhenNoSteps": "true",
            "Ec2SubnetId": "",
            "EmrManagedMasterSecurityGroup": "",
            "EmrManagedSlaveSecurityGroup": "",
            "ServiceAccessSecurityGroup": "",
            "InstanceFleets": [
            {
                "Name": "MASTER",
                "InstanceFleetType": "MASTER",
                "TargetOnDemandCapacity": 1,
                "TargetSpotCapacity": 0,
                "InstanceTypeConfigs": [
                    { 
                        "InstanceType": "m6g.xlarge",
                        "EbsConfiguration": {
                            "EbsOptimized": "true",
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "VolumeType": "gp3",
                                        "SizeInGB": 100
                                    },
                                    "VolumesPerInstance": 1
                                }
                            ]
                        }
                    }
            ]
            },
            {
                "Name": "CORE",
                "InstanceFleetType": "CORE",
                "TargetOnDemandCapacity": 0,
                "TargetSpotCapacity": 2,
                "LaunchSpecifications": {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": 10,
                        "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                        "AllocationStrategy": "CAPACITY_OPTIMIZED" 
                    }
                },
                "InstanceTypeConfigs": [
                    { 
                        "InstanceType": "m6g.xlarge", 
                        "WeightedCapacity": 1,
                        "EbsConfiguration": {
                            "EbsOptimized": "true",
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "VolumeType": "gp3",
                                        "SizeInGB": 100
                                    },
                                    "VolumesPerInstance": 1
                                }
                            ]
                        }
                    },
                    { 
                        "InstanceType": "m6g.2xlarge", 
                        "WeightedCapacity": 2,
                        "EbsConfiguration": {
                            "EbsOptimized": "true",
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "VolumeType": "gp3",
                                        "SizeInGB": 100
                                    },
                                    "VolumesPerInstance": 1
                                }
                            ]
                        }
                    }
                ]
            },
            {
                "Name": "TASK",
                "InstanceFleetType": "TASK",
                "TargetOnDemandCapacity": 0,
                "TargetSpotCapacity": 2,
                "LaunchSpecifications": {
                "SpotSpecification": {
                    "TimeoutDurationMinutes": 10,
                    "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                    "AllocationStrategy": "CAPACITY_OPTIMIZED" 
                }
                },
                "InstanceTypeConfigs": [
                    { 
                        "InstanceType": "m6g.xlarge", 
                        "WeightedCapacity": 1,
                        "EbsConfiguration": {
                            "EbsOptimized": "true",
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "VolumeType": "gp3",
                                        "SizeInGB": 100
                                    },
                                    "VolumesPerInstance": 1
                                }
                            ]
                        }
                    }
                ]
            }
        ]
        },
        "ManagedScalingPolicy": {
        "ComputeLimits": {
            "UnitType": "InstanceFleetUnits",
            "MinimumCapacityUnits": 1,
            "MaximumCapacityUnits": 4,
            "MaximumOnDemandCapacityUnits": 2,
            "MaximumCoreCapacityUnits": 2
        }
        },
        "Tags": [
            {
                "Key": "Teste",
                "Value": "valor"
            }
        ],
        "BootstrapActions": [
            {
                "Name": "base-bootstrap",
                "ScriptBootstrapAction": {
                    "Path": "s3://bucket/bootstrap_install.bash",
                    "Args": ["s3://bucket/requirements.txt"]
                }
            }
        ],
        "JobFlowRole": "EMR-InstanceProfile",
        "ServiceRole": "Role-EMR-Service",
        "AutoTerminationPolicy": {
            "IdleTimeout": 1800
        }
    }

    # Spark step configuration
    SPARK_STEP = [
        {
            'Name': 'ExecuteJob',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'client',
                    's3://quod-dev-confi-tools/scripts_jobs/save_hotlayer.py'
                ]
            }
        }
    ]

    # Create EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
    )

    # Add Spark step to EMR cluster
    add_spark_step = EmrAddStepsOperator(
        task_id='add_spark_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEP,
        aws_conn_id='aws_default',
    )

    # Wait for Spark step to complete
    wait_for_spark_step = EmrStepSensor(
        task_id='wait_for_spark_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    # Terminate EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    # Define task dependencies
    create_emr_cluster >> add_spark_step >> wait_for_spark_step >> terminate_emr_cluster
