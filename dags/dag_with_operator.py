from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "sanjeev",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}


def greet():
    print("Hello Sanjeev")


def greet_with_args(age, ti):
    # name = ti.xcom_pull()
    first_name = ti.xcom_pull(task_ids="get_name", key='first_name')
    last_name = ti.xcom_pull(task_ids="get_name", key='last_name')
    print("Hello My name is {} {} and I am  {} years old".format(first_name, last_name, age))


def get_name(ti):
    ti.xcom_push(key='first_name', value='Sanjeev V')
    ti.xcom_push(key='last_name', value='Goudar')
    # return "Sanju"


def get_age(ti):
    ti.xcom_push(key='age', value=28)


with DAG(
        default_args=default_args,
        dag_id="operator_v3_data_sharing_v2",
        description="Python Operator",
        start_date=datetime(2024, 4, 24),
        schedule_interval="@daily"
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet_with_args,
        op_kwargs={"age": 28}
    )

    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id="get_age",
        python_callable=get_age
    )
    [task2, task3] >> task1
