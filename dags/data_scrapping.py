from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from dom_uz import main as dom_uz_main
from joymee import main as joymee_main
from local import main as local_main
from uybor import main as uybor_main
from olx import main as olx_main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'scrapping_main',
    default_args=default_args,
    description='A DAG to run main functions from multiple scripts',
    schedule_interval='@daily',
)

# Define PythonOperator tasks for each main function
run_dom_uz_task = PythonOperator(
    task_id='run_dom_uz_script',
    python_callable=dom_uz_main,
    dag=dag,
)

run_joymee_task = PythonOperator(
    task_id='run_joymee_script',
    python_callable=joymee_main,
    dag=dag,
)

run_local_task = PythonOperator(
    task_id='run_local_script',
    python_callable=local_main,
    dag=dag,
)

run_uybor_task = PythonOperator(
    task_id='run_uybor_script',
    python_callable=uybor_main,
    dag=dag,
)

run_olx_task = PythonOperator(
    task_id='run_olx_script',
    python_callable=olx_main,
    dag=dag,
)

# Set task dependencies
run_dom_uz_task >> run_joymee_task >> run_local_task >> run_uybor_task >> run_olx_task
