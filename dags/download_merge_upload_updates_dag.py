import os
import sys
from dags.dags.utils import geofabrik_utils
from dags.dags.utils import osmosis_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import utils as utils
from utils import files_utils, s3_utils
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator



DOWNLOAD_DIR = "/opt/airflow/downloads/"
S3_BUCKET_NAME = Variable.get('routes-dag-bucket')
CONTINENT = 'south-america'

def download_from_geofabrik(date_ini=None):
    return geofabrik_utils.download_continent_updates(CONTINENT, date_ini, DOWNLOAD_DIR)

def upload_to_s3(file_path):
    if file_path is not None:
        s3_folder = CONTINENT +"/"+ datetime.now().strftime("%d-%m-%Y")
        file_name = os.path.basename(file_path)
        s3_utils.upload_file(file_path, s3_folder + "/" + file_name, S3_BUCKET_NAME)

def cleanup_volume(file_path):
    print("Cleaning volume dir: " + file_path)
    files_utils.cleanup_volume(file_path)
   
default_args = {
    'owner': 'slf_routes',
    'description': 'Baixa atualizações no formato .osc do site Geofabrik e salva no S3 para consumo posterior.',
    'provide_context': True
}

with DAG('download_merge_and_upload_osc', 
         default_args=default_args,
         schedule_interval=None,
         start_date=datetime(2023,10,26)
         ) as download_update_dag:
    
    download_from_geofabrik_t = PythonOperator(
        task_id='download_from_geofabrik',
        python_callable=download_from_geofabrik,
        op_args=[datetime(2023,10,26)]
    )

    merge_osc_files_t = KubernetesPodOperator(
        name="osmosis-processor",
        cmds=["bash", "-cx"],
        arguments=osmosis_utils.sh_merge_osc("{{ ti.xcom_pull(task_ids='download_from_geofabrik') }}")
        image='334077612733.dkr.ecr.sa-east-1.amazonaws.com/routes/osmosis:latest',
        image_pull_secrets='aws-cred-new',
        startup_timeout_seconds=900,
        task_id="merge_osc_files_t",
        volumes=[volume],
        volume_mounts=[volume_mount],
        in_cluster=True,
        on_finish_action='delete_pod',
        deferrable=True
    )

    upload_to_s3_t = python_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_args=["{{ ti.xcom_pull(task_ids='merge_osc_files') }}"],
        provide_context=True
    )

    cleanup_volume_t = PythonOperator(
        task_id="cleanup_volume",
        python_callable=cleanup_volume,
        op_args=[DOWNLOAD_DIR],
        provide_context=True
    )

download_from_geofabrik_t >> merge_osc_files_t >> upload_to_s3_t >> cleanup_volume_t




