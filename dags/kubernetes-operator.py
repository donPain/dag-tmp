import subprocess


from airflow import DAG

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago



def osmosisTask(self, osmosisExecInfo):
    expectedReturnValue = '{"foo": "bar"\n, "buzz": 2}'
    osmosis_update_file_task = KubernetesPodOperator(
        name="osmosis-processor",
        cmds=["bash", "-cx"],
        arguments=["/osmosis/package/bin/osmosis " + osmosisExecInfo, "'echo \'{}\' > /airflow/xcom/return.json'.format(expectedReturnValue)]"],
        image='334077612733.dkr.ecr.sa-east-1.amazonaws.com/routes/osmosis:latest',
        image_pull_secrets='aws-cred-new',
        startup_timeout_seconds=900,
        do_xcom_push=True,
        task_id="osmosis"
    )
    self.assertEqual(k.execute(None), json.loads(return_value))



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

    t1 = python_task = PythonOperator(
        task_id="python_task",
        python_callable=osmosisTask(self, "--help")
    )


   

    t1

