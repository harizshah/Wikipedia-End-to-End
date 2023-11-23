from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'hariz',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_xmltodict():
    import xmltodict
    print(f"xmltodict with version: {xmltodict.__version__} ")

with DAG(
    default_args=default_args,
    dag_id="dag_with_python_dependencies",
    start_date=datetime(2023, 11, 12),
    schedule_interval='@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id='get_xmltodict',
        python_callable=get_xmltodict
    )