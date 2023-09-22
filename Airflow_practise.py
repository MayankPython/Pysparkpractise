from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.sensors.http_sensor import Httpsensor
from datetime import timedelta

dag = DAG(
    dag_id="hello_dag",
    schedule_interval="@daily",
    start_date=days_ago(1)
)

task1 = BashOperator(
    task_id="t1",
    bash_command="{{var.value.mycommand}}",
    dag=dag
)

task2 = BashOperator(
    task_id="t2",
    bash_command="echo t2",
    dag=dag
)

task3 = BashOperator(
    task_id="t3",
    bash_command="echo t3",
    trigger_rule="all_failed",
    dag=dag
)

sensor = Httpsensor(
    task_id = "sensor",
    endpoint = "/",
    http_conn_id = "my_httpcon",
    dag = dag,
    retries = 20,
    retry_delay = timedelta(seconds=10)
)
#task1 run first and then 2 and 3 runs in parallel
sensor >> task1 >> [task2, task3]
