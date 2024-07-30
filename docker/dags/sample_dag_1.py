# Imports
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import requests
import csv
import os

url = "https://www.cryptodatadownload.com/cdd/Gemini_BTCUSD_2022_1min.csv"

def get_data(**kwargs):
    with requests.Session() as s:
        download = s.get(url)
        decoded_content = download.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)[2:]
        return my_list

def transform_data(**kwargs):
    ti = kwargs['ti']
    BTC_gemini_data = ti.xcom_pull(key=None, task_ids=['fetch_btc_gemini'])[0]
    now = datetime.now().strftime("%m%d%Y%H%M%S")
    price = []
    date = []

    for element in BTC_gemini_data:
        time = element[1]
        time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
        p = float(element[-2])
        date.append(time)
        price.append(p)


    folder_path = "/all_data"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path, mode=0o777)
        print(f"Folder '{folder_path}' created with chmod 777.")
    else:
        print(f"Folder '{folder_path}' already exists.")

    d = {"Date":date,"Price":price}
    df = pd.DataFrame(data=d)
    table = pa.Table.from_pandas(df)
    file_name = "/all_data/data"+now+".parquet"
    pq.write_table(table, file_name)

default_args = {
    'owner': 'roxsross',
    'start_date': datetime(2021, 10, 4, 11, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('crypto_1',
         catchup = False,
         default_args = default_args,
        #  schedule_interval = '*/1 * * * *',
         schedule_interval=None,
         ) as dag:
    fetch_binance_ohlcv = PythonOperator(task_id = 'fetch_btc_gemini',
                                         python_callable = get_data)
    transform_data = PythonOperator(task_id = 'transform_data',
                                    python_callable = transform_data)

fetch_binance_ohlcv >> transform_data