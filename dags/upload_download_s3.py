from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException

from datetime import datetime

CAMINHO_LOCAL_ARQUIVO = '/opt/airflow/downloads/859.osc.gz'
S3_BUCKET_NAME = 'bucket-feliphevs'
PASTA_NO_S3 = datetime.now().strftime("%d-%m-%Y")
NOME_DO_ARQUIVO_NO_S3 = '859.osc.gz'
S3_HOOK = S3Hook(aws_conn_id="aws_default")
caminho_objeto_s3 = PASTA_NO_S3 + NOME_DO_ARQUIVO_NO_S3

def upload_arquivo_para_s3():

    try:
        S3_HOOK.load_file(
        filename=CAMINHO_LOCAL_ARQUIVO,
        key=caminho_objeto_s3,
        bucket_name=S3_BUCKET_NAME
        )
        print("Arquivo carregado com sucesso no Amazon S3")
        return True
    except AirflowException as e:
        print(f"Erro ao carregar o arquivo para o Amazon S3: {str(e)}")
        return False

def download_s3(ti):

    update_upload_s3 = ti.xcom_pull(task_ids='upload_arquivo_s3')
    if update_upload_s3 is not None:
        print("update_upload_s3:" + str(update_upload_s3))

    if update_upload_s3:
        S3_HOOK.download_file(
        key=caminho_objeto_s3,
        bucket_name=S3_BUCKET_NAME,
        local_path="/opt/airflow/downloads",
        preserve_file_name=False
        )
        print("Arquivo baixado com sucesso no Amazon S3")
    else:
        print("Arquivo não encontrado no Amazon S3")
    

    
default_args = {
    "owner": "airflow",
    "description": "Upload de arquivo para o Amazon S3",
    "depend_on_past": False,
    "start_date": datetime(2023, 10, 19),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    'upload_download_s3',
    default_args=default_args,
    schedule_interval=None
)

upload_task = PythonOperator(
    task_id='upload_arquivo_s3',
    python_callable=upload_arquivo_para_s3,
    provide_context=True,
    dag=dag,
)

download_task = PythonOperator(
    task_id='download_task',
    python_callable=download_s3,
    provide_context=True,
    dag=dag,
)

upload_task >> download_task
