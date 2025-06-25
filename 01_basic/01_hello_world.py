from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['contact@shantanukhond.me'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='my_first_airflow_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=['hello_world', 'learning'],
    description='A simple DAG to say hello and print date.'
) as dag:
    start_task = BashOperator(
        task_id='start_greeting',
        bash_command='echo "Hello Airflow User! Starting our first DAG."',
    )

    print_date_task = BashOperator(
        task_id='print_current_date',
        bash_command='echo "Today\'s date is: $(date)"',
    )

    end_task = BashOperator(
        task_id='finish_greeting',
        bash_command='echo "Our first DAG has finished successfully!"',
    )

    start_task >> print_date_task >> end_task