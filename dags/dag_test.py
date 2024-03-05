import datetime

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from function.func import write_to_log

with DAG(
    dag_id='my_first_dag_ins',
    schedule_interval=None,
    start_date=pendulum.today(tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=5),
    tags=['MyFirst!'],
) as dag:

    my_fist_py_op_task = PythonOperator(
        task_id='my_fist_py_op_task',
        python_callable=write_to_log,
        op_kwargs={
            "warning_text": "warning!",
            "info_text": "info!",
            "debug_text": "debug!"
        }
    )
