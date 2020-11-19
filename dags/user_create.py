# -*- coding: utf-8 -*-
#
"""
### Example HTTP operator and sensor
"""
import json
from datetime import timedelta

import airflow
import logging
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from operators.status_http_operator import StatusHttpOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


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

code="{{ dag_run.conf['code'] }}"
rEmail="{{ dag_run.conf['realEmail'] }}"

kcData = {
        'firstName': 'User',
         'lastName': "{{ dag_run.conf['code'] }}",
         'email': f'user{code}@{domain}', 
         'enabled':True, 
         'username': f'user{code}',
         'emailVerified':True,
         'credentials': [{'type':'password','value':f'password{code}' ,'temporary': False}],
         'attributes': {'realEmail':  f'{rEmail}'} ,
         'realmRoles': [ 'student' ]
}

dag = DAG('user_create',
        schedule_interval=None, 
        default_args=default_args,
        tags=['validation','partners'],
        catchup=False,
        user_defined_filters={'fromjson': lambda s: json.loads(s)}
        )

dag.doc_md = __doc__

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


# def parse_conf(ds, **kwargs):
#     cc_ = kwargs['dag_run'].conf
#     code=cc_["code"]
#     rEmail=cc_["realEmail"]
#     res= {
#         "firstName": "User",
#         "lastName": f"{code}", 
#         "email": f"user{code}@{domain}", 
#         "enabled":True, 
#         "username": f"user{code}",
#         "emailVerified":True,
#         "credentials": [{"type":"password","value":f"password{code}" ,"temporary": False}],
#         "attributes": {"realEmail":  f"{rEmail}" } 
#         }
#     logging.info(res)
#     return res

# py_parseConf = PythonOperator(
#     task_id='py_parseConf',
#     provide_context=True,
#     python_callable=parse_conf,
#     dag=dag)

kc_createUser=SimpleHttpOperator(
    http_conn_id='kc_connection',
    task_id='kc_createUser',
    method='POST',
    endpoint=f'/auth/admin/realms/{kc_realm}/users',
    data=json.dumps(kcData),
    headers={'Content-Type': 'application/json', 'Authorization':'Bearer '+'{{ (task_instance.xcom_pull(key="return_value", task_ids="kc_token")| fromjson)["access_token"] }}' },
    response_check=lambda response: True if  response.status_code < 400 else False,
    log_response= True,
    xcom_push=True,
    dag=dag)

# kc_get_user_data = SimpleHttpOperator(
#     http_conn_id='kc_connection',
#     task_id='kc_get_user_data',
#     method='GET',
#     endpoint=f'/auth/admin/realms/{kc_realm}/users',
#     data=f'username={kcData["username"]}',
#     headers={'Content-Type': 'application/x-www-form-urlencoded', 'Authorization':'Bearer '+'{{(task_instance.xcom_pull(key="return_value", task_ids="kc_token")| fromjson)["access_token"] }}' },
#     response_check=lambda response: True if  response.status_code < 400 else False,
#     xcom_push=True,
#     trigger_rule=TriggerRule.ALL_DONE, #execute regardless of upstream failure
#     dag=dag)

# kc_enable_user = SimpleHttpOperator(
#     http_conn_id='kc_connection',
#     task_id='kc_enable_user',
#     method='PUT',
#     data=json.dumps(kcData),
#     endpoint=f'/auth/admin/realms/{kc_realm}/users/'+'{{(task_instance.xcom_pull(key="return_value", task_ids="kc_get_user_data")| fromjson)[0]["id"] }}',
#     headers={'Content-Type': 'application/json','Authorization':'Bearer '+'{{(task_instance.xcom_pull(key="return_value", task_ids="kc_token")| fromjson)["access_token"] }}' },
#     response_check=lambda response: True if  response.status_code < 400 else False,
#     xcom_push=True,
#     dag=dag)

sensor = HttpSensor(
    task_id='http_kc_check',
    http_conn_id='kc_connection',
    endpoint='',
    request_params={},
    response_check=lambda response: True if len(response.content)>=0 else False,
    poke_interval=5,
    dag=dag)

mail_create=SimpleHttpOperator(
    http_conn_id='mailbox_connection',
    task_id='mail_createMbox',
    method='POST',
    endpoint='/admin/mail/users/add',
    data=f'email=user{code}@{domain}&password=password{code}',
    headers={'Content-Type':'application/x-www-form-urlencoded'},
    response_check=lambda response: True if  (response.status_code < 400 or response.status_code == 409)  else False,
    dag=dag)

email_notify = EmailOperator(
        task_id='email_notify',
        to='amesegue@akamai.com',
        subject=f'Airflow: user{code} created',
        html_content=f'<h3>User: user{code} password: password{code}</h3>',
        dag=dag
)

sensor >> kc_token >> kc_createUser  >> email_notify

[kc_createUser,mail_create] >> email_notify

