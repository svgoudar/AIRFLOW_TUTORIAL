from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.decorators import dag, task

default_args = {
    "owner": "sanjeev",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}


@dag(dag_id="taskflow_api_dag_v1", default_args=default_args, start_date=datetime(2024, 4, 24), schedule_interval="@daily")
def hello_world():
    @task(multiple_outputs=True)
    def get_name():
        # return "SANJEEV"
        return {"first_name": "Sanjeev V ", "last_name": "Goudar"}

    @task()
    def get_age():
        return 19

    @task()
    def greet(first_name,last_name, age):
        print(f"Hello world My name is {first_name} {last_name} , I am {age} years old")

    name = get_name()
    age = get_age()
    # greet(name=name, age=age)
    greet(first_name=name["first_name"], last_name=name["last_name"], age=age)


greet_dag = hello_world()
