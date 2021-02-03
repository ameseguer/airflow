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
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup':False,
    'start_date':airflow.utils.dates.days_ago(2),
    'email': ['amesegue@akamai.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


cm_api_key=Variable.get('CM_API_KEY')
cm_api_sec=Variable.get('CM_API_SECRET')
timestamp=int(datetime.datetime.utcnow().timestamp())
concat=f'{cm_api_key}{cm_api_sec}{timestamp}'
signature=hashlib.md5(concat.encode()).hexdigest()

cm_list="{{dag_run.conf['cm_list']}}"
username="{{dag_run.conf['username']}}"
post_body=[]
post_body.append(username)

dag = DAG('cm_enroll',
        schedule_interval=None, 
        default_args=default_args,
        tags=['classmarker'],
        catchup=False,
        user_defined_filters={'fromjson': lambda s: json.loads(s)}
        )

cm_enrollment = SimpleHttpOperator(
    http_conn_id='classmarker_api',
    task_id='cm_',
    method='POST',
    endpoint=f'/v1/accesslists/{cm_list}.json?api_key={cm_api_key}&signature={signature}&timestamp={timestamp}',
    data=json.dumps(post_body),
    headers={"Content-Type": "application/json"},
    response_check=lambda response: True if ('status' in response.json() and response.json()['status']=='ok') else False,
    log_response=True,
    dag=dag)
