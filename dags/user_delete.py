# -*- coding: utf-8 -*-
#
"""
### Example HTTP operator and sensor
"""
import json
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

kc_secret=Variable.get('KEYCLOAK_SECRET')
kc_realm=Variable.get('KEYCLOAK_REALM')
kc_client=Variable.get('KEYCLOAK_CLIENT')

mail_admin=Variable.get('MAIL_ADMIN')
mail_passw=Variable.get('MAIL_ADMIN_PASS')

domain=Variable.get('DOMAIN')

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

dag = DAG('user_delete',
        schedule_interval=None, 
        default_args=default_args,
        tags=['validation','partners'],
        catchup=False,
        user_defined_filters={'fromjson': lambda s: json.loads(s)}
        )

dag.doc_md = __doc__

username='{{ dag_run.conf["username"] }}'

sensor = HttpSensor(
    task_id='http_kc_check',
    http_conn_id='kc_connection',
    endpoint='',
    request_params={},
    response_check=lambda response: True if len(response.content)>=0 else False,
    poke_interval=5,
    dag=dag)

kc_token = SimpleHttpOperator(
    http_conn_id='kc_connection',
    task_id='kc_token',
    method='POST',
    endpoint='/auth/realms/master/protocol/openid-connect/token',
    data='client_id='+kc_client+'&grant_type='+'client_credentials'+'&client_secret='+kc_secret,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    response_check=lambda response: True if 'access_token' in response.json() else False,
    xcom_push=True,
    log_response=True,
    dag=dag)

kc_get_user_data = SimpleHttpOperator(
    http_conn_id='kc_connection',
    task_id='kc_get_user_data',
    method='GET',
    endpoint=f'/auth/admin/realms/{kc_realm}/users',
    data=f'username={username}',
    headers={'Content-Type': 'application/x-www-form-urlencoded', 'Authorization':'Bearer '+'{{(task_instance.xcom_pull(key="return_value", task_ids="kc_token")| fromjson)["access_token"] }}' },
    response_check=lambda response: True if  response.status_code < 400 else False,
    xcom_push=True,
    dag=dag)

kc_delete_user = SimpleHttpOperator(
    http_conn_id='kc_connection',
    task_id='kc_delete_user',
    method='DELETE',
    endpoint=f'/auth/admin/realms/{kc_realm}/users/'+'{{(task_instance.xcom_pull(key="return_value", task_ids="kc_get_user_data")| fromjson)[0]["id"] }}',
    headers={'Authorization':'Bearer '+'{{(task_instance.xcom_pull(key="return_value", task_ids="kc_token")| fromjson)["access_token"] }}' },
    response_check=lambda response: True if  response.status_code < 400 else False,
    xcom_push=True,
    dag=dag)

mail_delete=SimpleHttpOperator(
    http_conn_id='mailbox_connection',
    task_id='mail_delete',
    method='POST',
    endpoint='/admin/mail/users/remove',
    data=f'email={username}@{domain}',
    headers={'Content-Type':'application/x-www-form-urlencoded'},
    response_check=lambda response: True if  response.status_code < 400 else False,
    dag=dag)

email_notify = EmailOperator(
        task_id='email_notify',
        to='amesegue@akamai.com',
        subject=f'Airflow: User {username} deleted',
        html_content=f'<h3>User {username} has been deleted</h3>',
        dag=dag
)

sensor >> kc_token >> kc_get_user_data >> kc_delete_user

[kc_delete_user,mail_delete] >> email_notify
