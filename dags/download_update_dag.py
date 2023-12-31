import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import utils as utils
from utils import geofabrik, files_utils, s3_utils
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


download_dir = "/opt/airflow/workdir/downloads/"
S3_BUCKET_NAME = 'routes-dag-exec'

def download_from_geofabrik(continent, date_ini=None):
    return geofabrik.download_continent_updates(continent, date_ini, download_dir)

def upload_to_s3(file_path, continent):
    s3_folder = continent +"/"+ datetime.now().strftime("%d-%m-%Y")
    for root, _, files in os.walk(file_path): 
        for file_name in files:
            final_file = os.path.join(root,file_name)
            s3_utils.upload_file(final_file, s3_folder + "/" + file_name, S3_BUCKET_NAME)


def cleanup_volume(file_path):
    print("Cleaning volume dir: " + file_path)
    files_utils.cleanup_volume(file_path)
   

default_args = {
    'owner': 'slf_routes',
    'description': 'Baixa atualizações no formato .osc do site Geofabrik e salva no S3 para consumo posterior.',
    'provide_context': True
}

with DAG('download_and_save_osc', 
         default_args=default_args,
         schedule_interval=None,
         start_date=datetime(2023,10,26)
         ) as download_update_dag:

    download_from_geofabrik_t = PythonOperator(
        task_id='download_from_geofabrik',
        python_callable=download_from_geofabrik,
        op_args=["south-america", datetime(2023,10,26)]
    )

    upload_to_s3_t = python_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_args=["{{ ti.xcom_pull(task_ids='download_from_geofabrik') }}","south-america"],
        provide_context=True
    )

    cleanup_volume_t = PythonOperator(
        task_id="cleanup_volume",
        python_callable=cleanup_volume,
        op_args=["{{ ti.xcom_pull(task_ids='download_from_geofabrik') }}"],
        provide_context=True
    )

    start_apply_dag_t = TriggerDagRunOperator(
        task_id="start_apply_dag",
        trigger_dag_id="apply_update",
        deferrable=True
    )


download_from_geofabrik_t >> upload_to_s3_t >> cleanup_volume_t >> start_apply_dag_t

