import subprocess


from airflow import DAG

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "don",
    "description": "Fetches and stores ECR credentials to allow Docker daemon to pull images",
    "depend_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
        "kubernetes_osmosis",
        default_args=default_args,
        schedule_interval="@hourly",
        catchup=False,
) as dag:




    osmosis_update_file_task = KubernetesPodOperator(
        name="osmosis-processor",
        cmds=["bash", "-cx"],
        arguments=["/osmosis/package/bin/osmosis --help"],
        image='334077612733.dkr.ecr.sa-east-1.amazonaws.com/routes/osmosis:latest',
        image_pull_secrets='aws-cred-new',
        startup_timeout_seconds=900,
        task_id="osmosis",
        do_xcom_push=True
    )

    t_1 = BashOperator(
        task_id="b4s",
        bash_command='echo "After Osmosis"'
    )

    osmosis_update_file_task() >> t_1

