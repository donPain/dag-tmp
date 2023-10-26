from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from datetime import datetime


S3_HOOK = S3Hook(aws_conn_id="aws_default")
WORKDIR_PATH="/opt/airflow/workdir"
EXEC_DATE= datetime.now().strftime("%d-%m-%Y")



def upload_file(local_path, key, bucket):
    try:
        if not S3_HOOK.check_for_key(key, bucket):
            S3_HOOK.load_file(
                filename=local_path,
                key=key,
                bucket_name=bucket
            )
            print(f"Arquivo {key} carregado com sucesso no Amazon S3")
        else:
            print(f'O arquivo {key} já existe no S3. Não será substituído.')
    except AirflowException as e:
        print(f"Erro ao carregar o arquivo {key} para o Amazon S3: {str(e)}")


def download_all_files_from_folder(s3_folder, output_folder, bucket):
     print(f"s3_folder -> {s3_folder}\n output_folder -> {output_folder}\n bucket -> {bucket}")
     objects = S3_HOOK.list_keys(prefix=s3_folder, bucket_name=bucket)
     for obj_key in objects:
        print("key -> " + obj_key)
        download_file(obj_key, output_folder, bucket)



def download_file(key, output, bucket):

    try:
        S3_HOOK.download_file(
            key=key,
            bucket_name=bucket,
            local_path=output,
            preserve_file_name=False
            )
    except AirflowException as e:
        print(f"Erro ao baixar o arquivo {key} do Amazon S3: {str(e)}")
    
