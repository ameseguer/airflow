import json
from datetime import timedelta

import airflow
import logging
import hashlib
import datetime
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['amesegue@akamai.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


cm_api_key = Variable.get('CM_API_KEY')
cm_api_sec = Variable.get('CM_API_SECRET')
mail_to = Variable.get('MAIL_NOTIFY_TO')


timestamp = int(datetime.datetime.utcnow().timestamp())
concat = f'{cm_api_key}{cm_api_sec}{timestamp}'
signature = hashlib.md5(concat.encode()).hexdigest()

# realEmail
# cm_list
# message

username = "{{dag_run.conf['username']}}"
message = "{{dag_run.conf['message']}}"
rEmail = "{{dag_run.conf['rEmail']}}"
cm_list = "{{dag_run.conf['cm_list']}}"
title = "{{dag_run.conf['title']}}"
attemptId = "{{dag_run.conf['attemptId']}}"
notifyEmail = "{{dag_run.conf['notifyEmail']}}"

post_body = []
post_body.append(username)

with DAG('lvs_endlab',
         schedule_interval=None,
         default_args=default_args,
         tags=['mysql', 'validation', 'classmarker'],
         catchup=False,
         user_defined_filters={'fromjson': lambda s: json.loads(s)}
         ) as dag:

    # with TaskGroup("cm_check", tooltip="Tasks for section_1") as cm_check:
    cm_enroll = SimpleHttpOperator(
        http_conn_id='classmarker_api',
        task_id='cm_enroll',
        method='POST',
        endpoint=f'/v1/accesslists/{cm_list}.json?api_key={cm_api_key}&signature={signature}&timestamp={timestamp}',
        data=json.dumps(post_body),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: True if (
            'status' in response.json() and response.json()['status'] == 'ok') else False,
        log_response=True,
        dag=dag)

    def dag_run_check(**kwargs):
        if kwargs['dag_run'].conf.get('cm_list'):
            return 'cm_enroll'
        else:
            return 'do_nothing'

    do_nothing = DummyOperator(task_id='do_nothing', dag=dag)

    branch_check = BranchPythonOperator(
        task_id='branch_check',
        python_callable=dag_run_check,
        dag=dag
    )

    # branch_check >> [do_nothing, cm_enroll]

    email_notify = EmailOperator(
        task_id='email_notify',
        to=f"{notifyEmail}{mail_to}",
        subject=title,
        html_content=username,
        dag=dag,
        trigger_rule='one_success',
    )
    email_notify_user = EmailOperator(
        task_id='email_notify_user',
        to=rEmail,
        subject=title,
        html_content=message,
        dag=dag
    )

    mysql_mark_notified = MySqlOperator(
        sql=f"""UPDATE `Attempts` 
            SET `notified` =  1 
            WHERE `Attempts`.`id` = '{attemptId}';""",
        mysql_conn_id='validation_db',
        task_id='mysql_mark_notified',
        parameters={"id": attemptId},
        trigger_rule='none_failed',
        dag=dag)

    branch_check >> [cm_enroll,
                     do_nothing] >> email_notify >> email_notify_user >> mysql_mark_notified
