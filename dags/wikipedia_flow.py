import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

################ pipeliness ########

from pipelines.wikipedia_pipelines import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data

###########################################################

dag = DAG(
    dag_id='wikipedia_flow',
    default_args={
        "owner": "Hariz Shah",
        "start_date": datetime(2023, 10, 1),
    },
    schedule_interval=None,
    catchup=False
)

#Extracting
extract_data_from_wikipedia = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag
)


#Preprocessing
transform_wikipedia_data = PythonOperator(
    task_id = "transform_wikipedia_data",
    provide_context = True,
    python_callable = transform_wikipedia_data,
    dag=dag
)


#Write
write_wikipedia_data = PythonOperator(
    task_id = 'write_wikipedia_data',
    provide_context=True,
    python_callable = write_wikipedia_data,
    dag=dag
)

extract_data_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data

