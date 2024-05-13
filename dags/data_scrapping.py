from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

def execute_notebooks():
    scripts = [
        "/Users/didi/Desktop/data_scrapping/Data Scrapping/Scrapping/02.Dom_uz.py",
        "/Users/didi/Desktop/data_scrapping/Data Scrapping/Scrapping/03.Joymee.py",
        "/Users/didi/Desktop/data_scrapping/Data Scrapping/Scrapping/04.Local.py",
        "/Users/didi/Desktop/data_scrapping/Data Scrapping/Scrapping/05.Olx.py",
        "/Users/didi/Desktop/data_scrapping/Data Scrapping/Scrapping/06.Uybor.py"
    ]
    for script in scripts:
        subprocess.run(["/usr/local/bin/python3", script])

dag = DAG(
    'scrapping',
    catchup=False
)

execute_notebooks_task = PythonOperator(
    task_id='execute_scrapping',
    python_callable=execute_notebooks,
    dag=dag
)

execute_notebooks_task
