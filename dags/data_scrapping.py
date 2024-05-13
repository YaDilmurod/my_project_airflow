from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import subprocess

def execute_notebook():
    subprocess.run(["/usr/local/bin/python3", "scrapping_domuz.py"])

dag = DAG(
    'scrapping_dom_uz',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

execute_notebook_task = PythonOperator(
    task_id='execute_notebook',
    python_callable=execute_notebook,
    dag=dag
)

execute_notebook_task
