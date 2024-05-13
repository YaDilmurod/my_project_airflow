from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

def execute_notebook(script):
    subprocess.run(["/usr/local/bin/python3", script])

dag = DAG(
    'scrapping_test',
    catchup=False
)

scripts = [
    "/Users/didi/Desktop/data_scrapping/Data Scrapping/Code/Scrapping/dom_uz.py",
    "/Users/didi/Desktop/data_scrapping/Data Scrapping/Code/Scrapping/joymee.py",
    "/Users/didi/Desktop/data_scrapping/Data Scrapping/Code/Scrapping/local.py",
    "/Users/didi/Desktop/data_scrapping/Data Scrapping/Code/Scrapping/olx.py",
    "/Users/didi/Desktop/data_scrapping/Data Scrapping/Code/Scrapping/uybor.py",
    "/Users/didi/Desktop/data_scrapping/Data Scrapping/Code/Scrapping/grouping_sources.py",
    "/Users/didi/Desktop/data_scrapping/Data Scrapping/Code/eda.py"
]

for i, script in enumerate(scripts):
    task_id = f'execute_{i+1}_{script.split("/")[-1].split(".")[0]}'
    execute_task = PythonOperator(
        task_id=task_id,
        python_callable=execute_notebook,
        op_args=[script],
        dag=dag
    )
