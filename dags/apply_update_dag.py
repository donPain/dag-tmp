import subprocess
from kubernetes.client import models as k8s


from airflow import DAG

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago


UPDATE_DATABASE=True;
UPDATE_FILE=True;


volume = k8s.V1Volume(
    name="workdir-pv",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="airflow-claim"),
)

volume_mount = k8s.V1VolumeMount(
    name="workdir-pv", mount_path="/opt/airflow/workdir", sub_path=None, read_only=False
)


default_args = {
    'owner': 'slf_routes',
    'description': 'Utiliza os arquivos .osc para atualizar arquivo .pbf e banco de dados.',
}

with DAG(
        "apply_update",
        default_args=default_args,
        schedule_interval="@hourly",
        start_date=(2023,10,26),
        catchup=False,
) as dag:

    osmosis_update_file_task = KubernetesPodOperator(
        name="osmosis-processor",
        cmds=["bash", "-cx"],
        arguments=[
            "/osmosis/package/bin/osmosis --help"
            # "sleep 500"
        ],
        image='334077612733.dkr.ecr.sa-east-1.amazonaws.com/routes/osmosis:latest',
        image_pull_secrets='aws-cred-new',
        startup_timeout_seconds=900,
        reattach_on_restart=False,
        is_delete_operator_pod=True,
        task_id="osmosis",
        volumes=[volume],
        volume_mounts=[volume_mount]
    )

    # createTmp  = BashOperator(
    #     task_id="bash_task",
    #     bash_command='cat /opt/airflow/workdir/teste.txt'
    # )

    # # readTmp =  BashOperator(
    # #     task_id="bash_task_2",
    # #     bash_command='cat /opt/airflow/workdir/test.txt'
    # # )



    osmosis_update_file_task.dry_run()
