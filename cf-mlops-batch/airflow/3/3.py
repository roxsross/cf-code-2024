from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
from io import StringIO

# URL del archivo CSV
CSV_URL = 'https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv'

def download_csv(**kwargs):
    response = requests.get(CSV_URL)
    response.raise_for_status()  # Asegura que la solicitud se realizó con éxito
    return response.text

def process_csv(**kwargs):
    # Obtener el contenido del CSV desde XCom
    ti = kwargs['ti']
    csv_content = ti.xcom_pull(task_ids='download_csv')
    
    # Utiliza pandas para leer el CSV desde una cadena de texto
    df = pd.read_csv(StringIO(csv_content))
    # Mostrar los primeros registros en los logs
    print(df.head())

# Argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'csv_processing_pipeline2',
    default_args=default_args,
    description='Un DAG para procesar un archivo CSV desde una URL',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Definir las tareas
download_task = PythonOperator(
    task_id='download_csv',
    python_callable=download_csv,
    dag=dag,
    do_xcom_push=True  # Habilita el empuje de XCom para pasar datos entre tareas
)

process_task = PythonOperator(
    task_id='process_csv',
    python_callable=process_csv,
    dag=dag,
    provide_context=True  # Habilita el paso de contexto para acceder a XCom
)

# Definir las dependencias entre las tareas
download_task >> process_task
