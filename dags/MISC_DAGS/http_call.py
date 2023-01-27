import datetime

import pendulum

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.email_operator import EmailOperator
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email

def _final_status(**kwargs):
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.task_id != 'pipeline_fail' :
            if task_instance.current_state() != State.SUCCESS and \
                    task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception("Task {} failed. Failing this DAG run".format(task_instance.task_id))
with DAG(
    dag_id='http_call',
    schedule_interval='0 0 * * *',
    start_date=pendulum.yesterday(tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
    render_template_as_native_obj=True,
) as dag:
   data_interval_start = "{{ data_interval_start.to_rfc3339_string() }}"
   dag_name = "{{ dag }}"
   dag_run_id = "{{ run_id }}"
   dag_run = "{{ dag_run }}"
   dag_run_time = "{{ ts }}"

   task_http_sensor_check = HttpSensor(
    task_id="http_sensor_check",
    http_conn_id="http_conn",
    endpoint="api/temperature?name=cairns",
    request_params={},
    response_check=lambda response: "successfully" in response.text,
    poke_interval=5,
    dag=dag,
    timeout=3600,
    trigger_rule="all_success",
    
    )

   task_http_operator_check = SimpleHttpOperator(
    task_id="task_operator_check",
    http_conn_id="http_conn",
    method = 'GET',
    endpoint="api/temperature?name=sydney",
    headers={'Content-Type':'application/json'},
    dag=dag,    
    response_check=lambda response: "successfully" in response.text.lower(),
    trigger_rule="all_success",
)


   get_common_names = PostgresOperator(
        task_id="get_common_names",
        postgres_conn_id="postgres_conn",
        sql="SELECT public.get_common_actor_name();",
        parameters={},
        autocommit=True,
        trigger_rule="all_success",
    )

   start_slack_notification = SlackWebhookOperator(
        task_id="send_slack_notification",
        http_conn_id="slack_conn",
        message=f"Started! {dag_run} , Dag schedule Time: {data_interval_start}",
        
    )

   fail_slack_watcher = SlackWebhookOperator(
        task_id="pipeline_fail",
        http_conn_id="slack_conn",
        message=f"Failed! {dag_run} , Dag schedule Time: {data_interval_start}",
        trigger_rule=TriggerRule.ONE_FAILED,
        dag=dag
    )

   final_status = PythonOperator(
    task_id='final_status',
    provide_context=True,
    python_callable=_final_status,
    trigger_rule=TriggerRule.ALL_DONE, # Ensures this task runs even if upstream fails
    dag=dag,
)


   start_email = EmailOperator(
        task_id='send_start_email',
        to='airflowmonitoring.alerts@gmail.com',
        subject=f'Airflow Alert! Started {dag_run} , Dag schedule Time: {data_interval_start}',
        html_content= f"""Hi Team, <br><br>Started {dag_run} , Dag schedule Time: {data_interval_start} <br><br> Thank You. <br>""",
        dag=dag
   )

   success_email = EmailOperator(
        task_id='send_success_email',
        to='airflowmonitoring.alerts@gmail.com',
        subject=f'Airflow Alert! Success {dag_run} , Dag schedule Time: {data_interval_start}',
        html_content= f"""Hi Team, <br><br>Success {dag_run} , Dag schedule Time: {data_interval_start} <br><br> Thank You. <br>""",
        dag=dag,
        trigger_rule="all_success"
   )

   start_email >> start_slack_notification >> get_common_names >> task_http_operator_check >> task_http_sensor_check >> success_email >> fail_slack_watcher >> final_status