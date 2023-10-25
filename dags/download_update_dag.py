import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException

base_url = 'http://download.geofabrik.de/'
download_dir = "/opt/airflow/workdir/"
S3_BUCKET_NAME = 'routes-dag-exec'
S3_HOOK = S3Hook(aws_conn_id="aws_pessoal")

def download_from_geofabrik(**context, continent, date_ini=None):
    ti = context["ti"]
    today_date = datetime.now().strftime("%d-%m-%Y")
    download_dir_today = os.path.join(download_dir + "/" + continent, today_date)

    if not os.path.exists(download_dir_today):
        os.makedirs(download_dir_today)


    folder_link = get_folder_link(continent, date_ini)
    if folder_link:
        osc_links = get_osc_links(folder_link, date_ini)
        if osc_links:
            download_osc_files(ti, osc_links, download_dir_today)
            print("Downloads concluídos")
        else:
            print("Sem novas atualizações")
    else:
        print("Sem novas atualizações")

def download_osc_files(ti, osc_links, download_dir_today):
    for link in osc_links:
        import urllib.request
        file_name = os.path.join(download_dir_today, os.path.basename(link))  # Caminho completo do arquivo
        urllib.request.urlretrieve(link, file_name)
    
    ti.xcom_push(key="task-dir", value=download_dir_today)


def get_folder_link(continent, date_ini=None):
    url_base = f'{base_url}{continent}-updates/000/'
    response_base = requests.get(url_base)

    if response_base.status_code == 200:
        soup = BeautifulSoup(response_base.text, 'html.parser')

        rows = soup.find_all('tr')[3:-1]

        latest_folders = [row for row in rows if datetime.strptime(row.find_all('td')[2].get_text().strip(), "%Y-%m-%d %H:%M") >= date_ini]

        if latest_folders:
            folder_rows = sorted(rows, key=lambda row: datetime.strptime(row.find_all('td')[2].get_text().strip(), "%Y-%m-%d %H:%M"), reverse=True)
            folder_link = folder_rows[0].find_all('td')[1].find('a', href=True)['href'].strip()
            return url_base + folder_link
        else:
            return None
    else:
        return None
    
def get_osc_links(folder_link, date_ini):

    response_folder = requests.get(folder_link)

    if response_folder.status_code == 200:
        soup = BeautifulSoup(response_folder.text, 'html.parser')

        rows = soup.find_all('tr')
        osc_rows = [row for row in rows if row.find('a', href=lambda href: href and href.endswith('.osc.gz'))]

        latest_files = [row for row in osc_rows if datetime.strptime(row.find_all('td')[2].get_text().strip(), "%Y-%m-%d %H:%M") >= date_ini]

        if latest_files:
            latest_files = sorted(latest_files, key=lambda row: datetime.strptime(row.find_all('td')[2].get_text().strip(), "%Y-%m-%d %H:%M"), reverse=False)

            osc_links = []

            for file in latest_files:         
                file_link = file.find_all('td')[1].find('a', href=True)['href'].strip()
                osc_links.append(folder_link + file_link)

            return osc_links
        else:
            return None
    else:
        return None
    

def upload_to_s3(ti):
    s3Folder = continent +"/"+ datetime.now().strftime("%d-%m-%Y")
    filePath = ti.xcom_pull(key="task-dir", task_ids='download_from_geofabrik')
    for root, dirs, files in os.walk(localDir):
        for file_name in files:
            print(file_name)
            # local_path = os.path.join(root, file_name)
            # s3_key = os.path.join(s3Folder, file_name)
            # try:
            #     S3_HOOK.load_file(
            #         filename=local_path,
            #         key=s3_key,
            #         bucket_name=S3_BUCKET_NAME
            #     )
            #     print(f"Arquivo {file_name} carregado com sucesso no Amazon S3")
            # except AirflowException as e:
            #     print(f"Erro ao carregar o arquivo {file_name} para o Amazon S3: {str(e)}")
        
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 18),
    'provide_context': True
}

dagRotas = DAG('download_update', default_args=default_args, schedule_interval=None)

download_from_geofabrik_t = PythonOperator(
    task_id='download_from_geofabrik',
    python_callable=download_from_geofabrik,
    op_args=["south-america", datetime(2023,10,20)],
    dag=dagRotas
)

upload_to_s3_t = python_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dagRotas
)


download_from_geofabrik_t >> upload_to_s3_t

