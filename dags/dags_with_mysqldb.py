from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='dag_with_postgres_v4',
        default_args=default_args,
        description='This is our first dag that we write to mysql',
        start_date=datetime(2024, 4, 26),
        schedule_interval='0 0 * * *',
) as dag:
    task1 = PostgresOperator(task_id="create_mysq_table", postgres_conn_id="postgres_id",
                             sql="""
                          create table if not exists airflow_table (
                            id int not null primary key,
                            dt date,
                            name varchar(30))
                          """)

    task2 = PostgresOperator(task_id="insert_into_table",postgres_conn_id="postgres_id",
                             sql="""
                             insert into airflow_table values (2,'{{ ds }}','{{ dag.dag_id }}');                             
                             """)

    task3 = PostgresOperator(task_id="delete_from_table", postgres_conn_id="postgres_id",
                             sql="""
                                 delete from  airflow_table where dt = '{{ ds }}' and name = '{{ dag.dag_id }}';                             
                                 """)

    task1 >> task3 >> task2