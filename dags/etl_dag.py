from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define DAG
dag = DAG(
    'etl_pipeline',
    description='ETL Pipeline',
    schedule_interval='0 12 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Define ETL function
def extract_data():
    # Add your extraction code here
    pass

def transform_data():
    # Add your transformation code here
    pass

def load_data():
    # Add your loading code here
    pass

# Create tasks
extract_task = PythonOperator(task_id='extract', python_callable=extract_data, dag=dag)
transform_task = PythonOperator(task_id='transform', python_callable=transform_data, dag=dag)
load_task = PythonOperator(task_id='load', python_callable=load_data, dag=dag)

# Task dependencies
extract_task >> transform_task >> load_task