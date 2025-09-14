import boto3
import json


emr_client = boto3.client('emr', region_name='us-east-1')

cluster_template_details = {
        "Name": "emr-cluster-with-custom-python",
        "ReleaseLabel": "emr-7.8.0",
        "Applications": [
        {
            "Name": "Spark"
        }
        ],
        "Instances": {
            "InstanceFleets": [
                {
                "InstanceFleetType": "MASTER",
                "TargetOnDemandCapacity": 1,
                "InstanceTypeConfigs": [
                    {
                    "InstanceType": "r6g.xlarge"
                    }
                ]
                },
                {
                "InstanceFleetType": "CORE",
                "TargetOnDemandCapacity": 0,
                "TargetSpotCapacity": 1,
                "InstanceTypeConfigs": [
                    {
                    "InstanceType": "r6g.xlarge"
                    }
                ]
                }
            ],
            "Ec2KeyName": "mykeypc",
            "Ec2SubnetId": "subnet-5087cd7e",
            "KeepJobFlowAliveWhenNoSteps": True
        },
        "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
            {
                "Classification": "export",
                "Properties": {
                "PYSPARK_PYTHON": "/usr/bin/python3.11"
                }
            }
            ]
        }
        ],
        "LogUri": "s3://aws-logs-074454926727-us-east-1/emr-logs/",
        "ServiceRole": "EMR_DefaultRole",
        "JobFlowRole": "EMR_EC2_DefaultRole"
    }

response = emr_client.run_job_flow(**cluster_template_details)

cluster_id = response['JobFlowId']
print(f"Started EMR cluster with ID: {cluster_id}")

steps_config = [
    {
        "Name": "Run Spark Example",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "client",
                "s3://movies-analytics/save_table_date.py"
            ]
        }
    }
]

response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=steps_config
    )
