from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
import datetime
import airflow
# from airflow.utils.dates import
# https://airflow.apache.org/code.html#airflow.models.BaseOperator
default_args = {
    "depends_on_past": False,
    "start_date": datetime.datetime.now(),
    "retries": 1,
    "retry_delay": datetime.timedelta(hours=5),
}

with airflow.DAG("file_sensor_test_v1", default_args=default_args, schedule_interval="*/5 * * * *", ) as dag:
    start_task = EmptyOperator(task_id="start")
    stop_task = EmptyOperator(task_id="stop")
    sensor_task = FileSensor(task_id="my_file_sensor_task", poke_interval=10,
                             filepath="/tmp/data.csv")

start_task >> sensor_task >> stop_task
