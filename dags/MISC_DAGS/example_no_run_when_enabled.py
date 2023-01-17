import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='example_no_run_when_enabled',
    schedule_interval='0 0 * * *',
    start_date=pendulum.today(tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
) as dag:

    run_this = BashOperator(
        task_id='run_this',
        bash_command='echo {{ ts }}; echo {{ data_interval_start }}; echo {{ data_interval_end }}',
    )
