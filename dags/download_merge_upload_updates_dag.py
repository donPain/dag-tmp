import os
from utils import geofabrik, files_utils, s3_utils, osmosis_command
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


DOWNLOAD_DIR = "/opt/airflow/downloads/"
S3_BUCKET_NAME = 'routes-dag-exec'
CONTINENT = 'south-america'

def download_from_geofabrik(date_ini=None):
    return geofabrik.download_continent_updates(CONTINENT, date_ini, DOWNLOAD_DIR)

def merge_update_files(download_dir):
    if download_dir is not None:
        file_paths = files_utils.get_osc_file_paths(download_dir)
        number_of_files = len(file_paths)
        if number_of_files > 0:
            file_path = osmosis_command.merge_osc_files(download_dir, file_paths, number_of_files)
            return file_path
        else:
            return None
    else:
        return None

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

    merge_osc_files_t = PythonOperator(
        task_id='merge_osc_files',
        python_callable=merge_update_files,
        op_args=["{{ ti.xcom_pull(task_ids='download_from_geofabrik') }}"]
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

