from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException

S3_HOOK = S3Hook(aws_conn_id="aws_default")

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