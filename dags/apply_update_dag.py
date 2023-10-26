from kubernetes.client import models as k8s
from utils import osmosis_command, s3_utils, files_utils;
from datetime import datetime
from airflow.operators.python import PythonOperator


from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


UPDATE_DATABASE=False;
UPDATE_FILE=True;
CONTINENT="south-america"
WORKDIR_PATH="/opt/airflow/workdir"
EXEC_DATE= datetime.now().strftime("%d-%m-%Y")
S3_BUCKET_NAME = 'routes-dag-exec'

volume = k8s.V1Volume(
    name="workdir-pv",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="airflow-claim"),
)

volume_mount = k8s.V1VolumeMount(
    name="workdir-pv", mount_path=WORKDIR_PATH, sub_path=None, read_only=False
)


default_args = {
    'owner': 'slf_routes',
    'description': 'Utiliza os arquivos .osc para atualizar arquivo .pbf e banco de dados.'
}

def download_from_s3():
    s3_folder = f"{CONTINENT}/{EXEC_DATE}"
    output = f"{WORKDIR_PATH}/{CONTINENT}/download/{EXEC_DATE}"
    files_utils.cleanup_volume(output)
    s3_utils.download_all_files_from_folder(s3_folder, output, S3_BUCKET_NAME)


with DAG(
        "apply_update",
        default_args=default_args,
        schedule_interval="@hourly",
        start_date=datetime(2023, 10, 19),
        catchup=False,
) as dag:

    download_from_s3_t = python_task = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3
    )

    osmosis_update_file_t = KubernetesPodOperator(
        name="osmosis-processor",
        cmds=["bash", "-cx"],
        arguments=[
            osmosis_command.apply_changes_pbf("/osmosis/package/bin/osmosis ", 
                                                f"{WORKDIR_PATH}/{CONTINENT}/{CONTINENT}-latest.osm.pbf",
                                                f"{WORKDIR_PATH}/{CONTINENT}/download/{EXEC_DATE}/861.osc.gz",
                                                f"{WORKDIR_PATH}/{CONTINENT}/{CONTINENT}-{EXEC_DATE}.osm.pbf")
        ],
        image='334077612733.dkr.ecr.sa-east-1.amazonaws.com/routes/osmosis:latest',
        image_pull_secrets='aws-cred-new',
        startup_timeout_seconds=900,
        reattach_on_restart=False,
        is_delete_operator_pod=True,
        task_id="osmosis_update_file_t",
        volumes=[volume],
        volume_mounts=[volume_mount],
        deferrable=True
    )

 


    download_from_s3_t >> osmosis_update_file_t
