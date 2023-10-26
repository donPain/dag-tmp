import subprocess

import boto3
from airflow import DAG
from airflow.models import Connection
from airflow import settings
from docker.types import Mount



from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago



def update_credentials():
    import logging
    import base64
    logger = logging.getLogger(__name__)
    aws_hook = AwsHook("aws_don")
    credentials = aws_hook.get_credentials()

    ecr = boto3.client(
        'ecr',
        region_name='sa-east-1',
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key
    )
    response = ecr.get_authorization_token()

    username, password = base64.b64decode(
        response['authorizationData'][0]['authorizationToken']
    ).decode('utf-8').split(':')

    print("UserName:" + username + "\npassword: " + password)

    registry_url = response['authorizationData'][0]['proxyEndpoint']
    
    session = settings.Session
    connection_name = "docker_ecr"
    try:
        connection_query = session.query(Connection).filter(Connection.conn_id == connection_name)
        connection_query_result = connection_query.one_or_none()
        if not connection_query_result:
            connection = Connection(conn_id=connection_name, conn_type="docker", host=registry_url,
                                    login=username, password=password)
            session.add(connection)
            session.commit()
        else:
            connection_query_result.host = registry_url
            connection_query_result.login = username
            connection_query_result.set_password(password)
            session.add(connection_query_result)
            session.commit()

    except Exception as e:
        logger.info("Failed creating connection")
        logger.info(e)




default_args = {
    "owner": "don",
    "description": "Fetches and stores ECR credentials to allow Docker daemon to pull images",
    "depend_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
        "run_osmosis",
        default_args=default_args,
        schedule_interval="@hourly",
        catchup=False,
) as dag:
    update_credentials_task = PythonOperator(
        task_id="update_credentials", python_callable=update_credentials
    )

    run_osmosis_from_ecr = DockerOperator(
        api_version='auto',
        docker_url='unix://var/run/docker.sock',
        tty=True,
        command='/osmosis/package/bin/osmosis --help',
        image='334077612733.dkr.ecr.sa-east-1.amazonaws.com/routes/osmosis:latest',
        network_mode='bridge',
        task_id='run_osmosis_from_ecr',
        privileged=True,
        docker_conn_id='docker_ecr',
        mount_tmp_dir=False
        # mounts=[
        #     Mount(
        #         source='/home/eduardo/work/git-reps/dev_2/rotas/v4/docker-compose/tmp',
        #         target='/tmp',
        #         type='bind'
        #     )
        # ]
    )
update_credentials_task >> run_osmosis_from_ecr