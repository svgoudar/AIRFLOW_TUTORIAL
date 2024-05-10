from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_scikit_learn():
    import sklearn
    print(f"Scikit learn {sklearn.__version__}")


default_args = {"owner": "code4j",
                "retry": 5,
                "retry_delay": timedelta(minutes=5)}

with DAG(default_args=default_args, dag_id="DAG_with_pytho_dependencie",
         start_date=datetime(2024, 5, 8), schedule_interval="@daily") as dag:
    get_sklearn = PythonOperator(task_id="sklearn", python_callable=get_scikit_learn)

    get_sklearn
