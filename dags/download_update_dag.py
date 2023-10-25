import os
import requests
import s3_utils
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


base_url = 'http://download.geofabrik.de/'
download_dir = "/opt/airflow/downloads/"
S3_BUCKET_NAME = 'bucket-feliphevs'
S3_HOOK = S3Hook(aws_conn_id="aws_default")

def download_from_geofabrik(continent, date_ini=None):
    today_date = datetime.now().strftime("%d-%m-%Y")
    download_dir_today = os.path.join(download_dir + continent, today_date)

    if not os.path.exists(download_dir_today):
        os.makedirs(download_dir_today)


    folder_link = get_folder_link(continent, date_ini)
    if folder_link:
        osc_links = get_osc_links(folder_link, date_ini)
        if osc_links:
            download_osc_files(osc_links, download_dir_today)
            print("Downloads concluídos")
            return download_dir_today
        else:
            print("Sem novas atualizações")
    else:
        print("Sem novas atualizações")

def download_osc_files(osc_links, download_dir_today):
    for link in osc_links:
        import urllib.request
        file_name = os.path.join(download_dir_today, os.path.basename(link))  # Caminho completo do arquivo
        urllib.request.urlretrieve(link, file_name)
    
    #ti.xcoms_push(key="task-dir", value=download_dir_today)
    


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
    
# def chamar_metodo(continent):

#     upload_to_s3(ti, continent)
    

def upload_to_s3(file_path, continent):
    s3_folder = continent +"/"+ datetime.now().strftime("%d-%m-%Y")
    print("file path: " + file_path)
    for root, dirs, files in os.walk(file_path):
        for file_name in files:
            print(file_name)
            s3_utils.upload_file(file_path + "/" + file_name, s3_folder + "/" + file_name, S3_BUCKET_NAME)
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
    op_args=["{{ ti.xcom_pull(task_ids='download_from_geofabrik') }}","south-america"],
    provide_context=True,
    dag=dagRotas
)


download_from_geofabrik_t >> upload_to_s3_t

