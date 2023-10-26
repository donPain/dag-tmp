import os
from utils import s3_utils
from utils import geofabrik
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


download_dir = "/opt/airflow/workdir/downloads/"
S3_BUCKET_NAME = 'routes-dag-exec'

def download_from_geofabrik(continent, date_ini=None):
    return geofabrik.download_continent_updates(continent, date_ini, download_dir)

def upload_to_s3(file_path, continent):
    s3_folder = continent +"/"+ datetime.now().strftime("%d-%m-%Y")
    print("file-path: " + file_path)
    for files in os.walk(file_path):
        for file_name in files:
            print(file_name)
            # s3_utils.upload_file(file_path + "/" + file_name, s3_folder + "/" + file_name, S3_BUCKET_NAME)
        
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 18),
    'provide_context': True
}

dagRotas = DAG('download_update', default_args=default_args, schedule_interval=None)

download_from_geofabrik_t = PythonOperator(
    task_id='download_from_geofabrik',
    python_callable=download_from_geofabrik,
    op_args=["south-america", datetime(2023,10,26)],
    dag=dagRotas
)

upload_to_s3_t = python_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    op_args=["{{ ti.xcom_pull(task_ids='download_from_geofabrik') }}","south-america"],
    provide_context=True,
    dag=dagRotas
)


download_from_geofabrik_t >> upload_to_s3_t

