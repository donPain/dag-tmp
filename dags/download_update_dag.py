import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from datetime import datetime

base_url = 'http://download.geofabrik.de/'
download_dir = "/opt/airflow/workdir/"

def download_community_updates(continent, date_ini=None):
    folder_link = get_folder_link(continent, date_ini)
    if folder_link:
        osc_links = get_osc_links(folder_link, date_ini)
        if osc_links:
            download_osc_files(osc_links)
            print("Downloads concluídos")
        else:
            print("Sem novas atualizações")
    else:
        print("Sem novas atualizações")

def download_osc_files(osc_links):
    for link in osc_links:
        import urllib.request
        file_name = download_dir + datetime.now().strftime("%d-%m-%Y") + os.path.basename(link)
        os.mkdir(file_name, 0o666)
        urllib.request.urlretrieve(link, file_name)


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
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 18)
}

dagRotas = DAG('download_update', default_args=default_args, schedule_interval=None)

download_update_task = PythonOperator(
    task_id='download_update_task',
    python_callable=download_community_updates,
    op_args=["south-america", datetime(2023,10,20)],
    dag=dagRotas
)

download_update_task

