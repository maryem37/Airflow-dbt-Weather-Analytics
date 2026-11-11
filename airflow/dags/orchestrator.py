from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def example_task():
    print("This is an example task")


default_args = {
    'description': 'A DAG to orchestrate data',
    'start_date': datetime(2025, 4, 30),
    'catchup': False,
}

dag = DAG(
    dag_id='weather-api-orchestrator',
    default_args=default_args,
    schedule=timedelta(minutes=1)
)

with dag:
    task1 = PythonOperator(
        task_id='example_task',
        python_callable=example_task
    )
    # task2