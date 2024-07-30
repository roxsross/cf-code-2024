from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Funciones de ejemplo para las tareas
def extract_data():
    print("Extrayendo datos...")

def transform_data():
    print("Transformando datos...")

def load_data():
    print("Cargando datos...")

# Definir los argumentos por defecto
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
    'data_pipeline',
    default_args=default_args,
    description='Un DAG para procesar datos (ETL)',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Definir las tareas
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

# Definir las dependencias entre las tareas
start_task >> extract_task >> transform_task >> load_task >> end_task