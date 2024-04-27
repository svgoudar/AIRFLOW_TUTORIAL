from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='dag_with_postgres_v1',
        default_args=default_args,
        description='This is our first dag that we write to mysql',
        start_date=datetime(2024, 4, 26),
        schedule_interval='0 0 * * *',
) as dag:
    task1 = PostgresOperator(task_id="create_mysq_table", postgres_conn_id="postgres_id",
                             sql="""
                          create table airflow_table (
                            id int not null primary key,
                            dt date,
                            name varchar(10 ),
                          )
                          """)

    task1