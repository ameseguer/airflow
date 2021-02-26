import json
import re
from datetime import timedelta

import airflow
import logging
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


kc_secret=Variable.get('KEYCLOAK_SECRET')
kc_realm=Variable.get('KEYCLOAK_REALM')
kc_client=Variable.get('KEYCLOAK_CLIENT')

mail_admin=Variable.get('MAIL_ADMIN')
mail_passw=Variable.get('MAIL_ADMIN_PASS')

domain=Variable.get('DOMAIN')
prefix=Variable.get('USER_PREFIX')
mail_to=Variable.get('MAIL_NOTIFY_TO')
mail_create=Variable.get('MAIL_CREATE')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup':False,
    'start_date':airflow.utils.dates.days_ago(2),
    'email': [mail_to],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

code="{{ dag_run.conf['code'] }}"
rEmail="{{ dag_run.conf['realEmail'] }}"
labName="{{ dag_run.conf['labName'] }}"

kcData = {
        'firstName': 'User',
         'lastName': "{{ dag_run.conf['code'] }}",
         'email': f'{prefix}{code}@{domain}', 
         'enabled':True, 
         'username': f'{prefix}{code}',
         'emailVerified':True,
         'credentials': [{'type':'password','value':f'password{code}' ,'temporary': False}],
         'attributes': {'realEmail':  f'{rEmail}'} ,
         'realmRoles': [ 'student' ]
}

with DAG('user_create',
        schedule_interval=None, 
        default_args=default_args,
        tags=['user_create','validation','keycloak', 'email'],
        catchup=False,
        user_defined_filters={'fromjson': lambda s: json.loads(s)}
        ) as dag:

    kc_token = SimpleHttpOperator(
        http_conn_id='kc_connection',
        task_id='kc_token',
        method='POST',
        endpoint='/auth/realms/master/protocol/openid-connect/token',
        data='client_id='+kc_client+'&grant_type='+'client_credentials'+'&client_secret='+kc_secret,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        response_check=lambda response: True if 'access_token' in response.json() else False,
        log_response=True,
        dag=dag)

    kc_createUser=SimpleHttpOperator(
        http_conn_id='kc_connection',
        task_id='kc_createUser',
        method='POST',
        endpoint=f'/auth/admin/realms/{kc_realm}/users',
        data=json.dumps(kcData),
        headers={'Content-Type': 'application/json', 'Authorization':'Bearer '+'{{ (task_instance.xcom_pull(key="return_value", task_ids="kc_token")| fromjson)["access_token"] }}' },
        response_check=lambda response: True if  response.status_code < 400 else False,
        log_response= True,
        dag=dag)

    sensor = HttpSensor(
        task_id='http_kc_check',
        http_conn_id='kc_connection',
        endpoint='',
        request_params={},
        response_check=lambda response: True if len(response.content)>=0 else False,
        poke_interval=5,
        dag=dag)
    
    def aka_mail(**kwargs):
        if mail_create == "False":
            return 'do_nothing'
        else:
            return 'mail_createMbox'

    do_nothing = DummyOperator(task_id='do_nothing', dag=dag)

    domain_check = BranchPythonOperator(
        task_id='domain_check',
        python_callable=aka_mail,
        dag=dag
        )

    mail_createMbox=SimpleHttpOperator(
        http_conn_id='mailbox_connection',
        task_id='mail_createMbox',
        method='POST',
        endpoint='/admin/mail/users/add',
        data=f'email={prefix}{code}@{domain}&password=password{code}',
        headers={'Content-Type':'application/x-www-form-urlencoded'},
        response_check=lambda response: True if  (response.status_code < 400 or response.status_code == 409)  else False,
        dag=dag)

     email_notify = EmailOperator(
             task_id='email_notify',
             to=mail_to,
             subject=f'Airflow: {prefix}{code} created',
             html_content=f'User:<h3> {prefix}{code}</h3><br/> password: <h3>password{code}</h3>',
             dag=dag
     )
     email_notify_user = EmailOperator(
             task_id='email_notify_user',
             to=rEmail,
             subject=f'Welcome to {labName}',
             html_content=f'You have been enrolled into {labName} with the following credentials<br/>User:<h3> {prefix}{code}</h3><br/> password: <h3>password{code}</h3>',
             dag=dag
     )

sensor >> kc_token >> kc_createUser  #>> email_notify >> email_notify_user


domain_check >> [mail_createMbox, do_nothing]
