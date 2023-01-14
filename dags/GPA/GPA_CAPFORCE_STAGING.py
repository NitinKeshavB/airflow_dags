from airflow import DAG
from airflow.decorators import task
import pendulum

## operators
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

##providers
from airflow.providers.databricks.operators.databricks import  DatabricksRunNowOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

##utils
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Define params for Run Now Operator
#notebook_params = {"src_sys_cd":"","table_name":""}


#Methods

def _final_status(**kwargs):
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.task_id != 'TSLACKFAIL' :
            if task_instance.current_state() != State.SUCCESS and \
                    task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception("Task {} failed. Failing this DAG run".format(task_instance.task_id))


with DAG(
    dag_id="GPA_CAPFORCE_STAGING",
    start_date=pendulum.datetime(2021, 1, 1, tz="Australia/Sydney"),
    schedule_interval="16 11 * * *",
    catchup=False,
	render_template_as_native_obj=True,
    default_args={
        "email_on_failure": False,
        "email_on_retry": False,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    data_interval_start = "{{ data_interval_start.to_rfc3339_string() }}"
    dag_name = "{{ dag }}"
    dag_run_id = "{{ run_id }}"
    dag_run = "{{ dag_run }}"
    dag_run_time = "{{ ts }}"
	
	##start task
    t0 = SlackWebhookOperator(
        task_id='START_SLACK_NOTIFCATION',
		http_conn_id="slack_conn",
		message=f"Started! {dag_run} , Dag schedule Time: {data_interval_start}",
    )
	
	##start task
    #t1 = EmailOperator(
	#	task_id='START_EMAIL_NOTIFCATION',
    #    to='airflowmonitoring.alerts@gmail.com',
    #    subject=f'Airflow Alert! Started {dag_run} , Dag schedule Time: {data_interval_start}',
    #    html_content= f"""Hi Team, <br><br>Started {dag_run} , Dag schedule Time: {data_interval_start} <br><br> Thank You. <br>""",
    #    dag=dag
    #)

    ##end_task
    tslackfail = SlackWebhookOperator(
        task_id="TSLACKFAIL",
        http_conn_id="slack_conn",
        message=f"Failed! {dag_run} , Dag schedule Time: {data_interval_start}",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

	##end_task
    tend = PythonOperator(
		task_id='FINAL_STATUS',
		provide_context=True,
		python_callable=_final_status,
		trigger_rule=TriggerRule.ALL_DONE,
	)
    
	##end_task
    #tsuccessemail = EmailOperator(
	#	task_id='TSUCCESSEMAIL',
    #    to='airflowmonitoring.alerts@gmail.com',
    #    subject=f'Airflow Alert! Success {dag_run} , Dag schedule Time: {data_interval_start}',
    #    html_content= f"""Hi Team, <br><br>Success {dag_run} , Dag schedule Time: {data_interval_start} <br><br> Thank You. <br>""",
    #    trigger_rule="all_success"
	#)
	
	##end_task
    tslacksuccess = SlackWebhookOperator(
        task_id="TSLACKSUCCESS",
        http_conn_id="slack_conn",
        message=f"Sucsess! {dag_run} , Dag schedule Time: {data_interval_start}",
        trigger_rule="all_success",
    )

    ##task
    GPA_CAPFORCE_STAGING_DB_CATEGORY = DatabricksRunNowOperator(
        task_id = "GPA_CAPFORCE_STAGING_DB_CATEGORY",
        databricks_conn_id = "databricks_conn",
        job_id = 627248161463014,
        notebook_params={"src_sys_cd" : "CAPF", "table_name" : "public.category"},
        trigger_rule="all_success",
    )

	##task
    GPA_CAPFORCE_STAGING_DB_CITY = DatabricksRunNowOperator(
        task_id = "GPA_CAPFORCE_STAGING_DB_CITY",
        databricks_conn_id = "databricks_conn",
        job_id = 627248161463014,
        notebook_params={"src_sys_cd" : "CAPF", "table_name" : "public.city"},
        trigger_rule="all_success",
    )

	##task
    GPA_CAPFORCE_STAGING_DB_COUNTRY = DatabricksRunNowOperator(
        task_id = "GPA_CAPFORCE_STAGING_DB_COUNTRY",
        databricks_conn_id = "databricks_conn",
        job_id = 627248161463014,
        notebook_params={"src_sys_cd" : "CAPF", "table_name" : "public.country"},
        trigger_rule="all_success",
    )

	##task
    GPA_CAPFORCE_STAGING_DB_ADDRESS = DatabricksRunNowOperator(
        task_id = "GPA_CAPFORCE_STAGING_DB_ADDRESS",
        databricks_conn_id = "databricks_conn",
        job_id = 627248161463014,
        notebook_params={"src_sys_cd" : "CAPF", "table_name" : "public.actor"},
        trigger_rule="all_success",
    )

	##task
    GPA_CAPFORCE_STAGING_DB_CUSTOMER = DatabricksRunNowOperator(
        task_id = "GPA_CAPFORCE_STAGING_DB_CUSTOMER",
        databricks_conn_id = "databricks_conn",
        job_id = 627248161463014,
        notebook_params={"src_sys_cd" : "CAPF", "table_name" : "public.customer"},
        trigger_rule="all_success",
    )

	##task
    GPA_CAPFORCE_LANDING__wait__GPA_CAPFORCE_LANDING_API_SOUTH = ExternalTaskSensor(
        task_id = "GPA_CAPFORCE_LANDING__wait__GPA_CAPFORCE_LANDING_API_SOUTH",
        external_dag_id = "GPA_CAPFORCE_LANDING",
        external_task_id = "GPA_CAPFORCE_LANDING_API_SOUTH",
        poke_interval = 60 ,
        timeout = 600 ,
        soft_fail = False ,
        execution_delta = timedelta(minutes=0),
        retries = 1 , 
    )
        ##Dependency setting
    t0 >> GPA_CAPFORCE_STAGING_DB_CATEGORY
    t0 >> GPA_CAPFORCE_STAGING_DB_CITY
    [GPA_CAPFORCE_STAGING_DB_CITY, GPA_CAPFORCE_LANDING__wait__GPA_CAPFORCE_LANDING_API_SOUTH] >> GPA_CAPFORCE_STAGING_DB_COUNTRY
    [GPA_CAPFORCE_STAGING_DB_COUNTRY, GPA_CAPFORCE_STAGING_DB_CITY, GPA_CAPFORCE_LANDING__wait__GPA_CAPFORCE_LANDING_API_SOUTH] >> GPA_CAPFORCE_STAGING_DB_ADDRESS
    [GPA_CAPFORCE_STAGING_DB_ADDRESS, GPA_CAPFORCE_STAGING_DB_COUNTRY, GPA_CAPFORCE_STAGING_DB_CITY, GPA_CAPFORCE_LANDING__wait__GPA_CAPFORCE_LANDING_API_SOUTH] >> GPA_CAPFORCE_STAGING_DB_CUSTOMER
        ##end tasks
    GPA_CAPFORCE_STAGING_DB_COUNTRY >> tslacksuccess  >> tslackfail >> tend
        ##end tasks
    GPA_CAPFORCE_STAGING_DB_ADDRESS >> tslacksuccess  >> tslackfail >> tend
        ##end tasks
    GPA_CAPFORCE_STAGING_DB_CUSTOMER >> tslacksuccess  >> tslackfail >> tend