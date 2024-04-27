from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='catchup_v3',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2024, 4, 20),
    schedule_interval='@daily',
    # catchup=True # To enable catchup i.e it will execute the DAG since the mentioned start date till now
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task with catchup!"
    )

    # task2 = BashOperator(
    #     task_id='second_task',
    #     bash_command="echo hey, I am task2 and will be running after task1!"
    # )
    #
    # task3 = BashOperator(
    #     task_id='thrid_task',
    #     bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2!"
    # )

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    # Task dependency method 3
    # task1 >> [task2, task3]