from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import subprocess

def execute_notebook():
    subprocess.run(["/usr/local/bin/python3", "/Users/didi/Desktop/data_scrapping/Data Scrapping/Scrapping/02.Dom_uz.py"])

def execute_notebook():
    subprocess.run(["/usr/local/bin/python3", "/Users/didi/Desktop/data_scrapping/Data Scrapping/Scrapping/03.Joymee.py"])
    
def execute_notebook():
    subprocess.run(["/usr/local/bin/python3", "/Users/didi/Desktop/data_scrapping/Data Scrapping/Scrapping/04.Local.py"])

def execute_notebook():
    subprocess.run(["/usr/local/bin/python3", "/Users/didi/Desktop/data_scrapping/Data Scrapping/Scrapping/05.Olx.py"])

def execute_notebook():
    subprocess.run(["/usr/local/bin/python3", "/Users/didi/Desktop/data_scrapping/Data Scrapping/Scrapping/06.Uybor.py"])

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
