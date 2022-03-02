import json
import re
import random
import string
from datetime import timedelta

import airflow
import logging
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


def randomword(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


kc_secret = Variable.get('KEYCLOAK_SECRET')
kc_realm = Variable.get('KEYCLOAK_REALM')
kc_client = Variable.get('KEYCLOAK_CLIENT')

mail_admin = Variable.get('MAIL_ADMIN')
mail_passw = Variable.get('MAIL_ADMIN_PASS')

domain = Variable.get('DOMAIN')
prefix = Variable.get('USER_PREFIX')
mail_to = Variable.get('MAIL_NOTIFY_TO')
mail_err = Variable.get('MAIL_ERR')
mail_create = Variable.get('MAIL_CREATE')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': [mail_err],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

realEmail = "{{ dag_run.conf['realEmail'] }}"
labName = "{{ dag_run.conf['labName'] }}"
username = "{{ dag_run.conf['username'] }}"
password = randomword(8)


kcData = {
    'firstName': 'Student',
    'lastName': username,
    'email': f'{username}@{domain}',
    'enabled': True,
    'username': username,
    'emailVerified': True,
    'credentials': [{'type': 'password', 'value': f'{password}', 'temporary': False}],
    'attributes': {'realEmail':  f'{realEmail}'},
    'realmRoles': ['student']
}

with DAG('user_create',
         schedule_interval=None,
         default_args=default_args,
         tags=['user_create', 'validation', 'keycloak', 'email'],
         catchup=False,
         user_defined_filters={'fromjson': lambda s: json.loads(s)},
         max_active_runs=1
         ) as dag:

    kc_token = SimpleHttpOperator(
        http_conn_id='kc_connection',
        task_id='kc_token',
        method='POST',
        endpoint=f'/auth/realms/master/protocol/openid-connect/token',
        data='client_id='+kc_client+'&grant_type=' +
        'client_credentials'+'&client_secret='+kc_secret,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        response_check=lambda response: True if 'access_token' in response.json() else False,
        log_response=True,
        dag=dag)

    kc_checkUser = SimpleHttpOperator(
        http_conn_id='kc_connection',
        task_id='kc_checkUser',
        method='GET',
        endpoint=f'/auth/admin/realms/{kc_realm}/users?username={username}',
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' +
                 '{{ (task_instance.xcom_pull(key="return_value", task_ids="kc_token")| fromjson)["access_token"] }}'},
        response_check=lambda response: True if (
            response.status_code < 400 or response.status_code == 409) else False,
        log_response=True,
        dag=dag)

    def userExists_func(ti):
        xcom_value = str(ti.xcom_pull(task_ids="kc_checkUser"))
        json_xcom = json.loads(xcom_value)

        if len(json_xcom) == 0:
            return "kc_createUser"
        else:
            return "do_nothing"

    kc_userExists = BranchPythonOperator(
        task_id="kc_userExists",
        python_callable=userExists_func,
        trigger_rule='all_done',
        dag=dag,
    )

    kc_createUser = SimpleHttpOperator(
        http_conn_id='kc_connection',
        task_id='kc_createUser',
        method='POST',
        endpoint=f'/auth/admin/realms/{kc_realm}/users',
        data=json.dumps(kcData),
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' +
                 '{{ (task_instance.xcom_pull(key="return_value", task_ids="kc_token")| fromjson)["access_token"] }}'},
        response_check=lambda response: True if (
            response.status_code < 400 or response.status_code == 409) else False,
        log_response=True,
        # trigger_rule='one_failed',
        dag=dag)

    sensor = HttpSensor(
        task_id='http_kc_check',
        http_conn_id='kc_connection',
        endpoint='',
        request_params={},
        response_check=lambda response: True if len(
            response.content) >= 0 else False,
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
    # Mail cow returns a dataset way to big

    # mail_checkMbox = SimpleHttpOperator(
    #     http_conn_id='mailbox_connection',
    #     task_id='mail_checkMbox',
    #     method='GET',
    #     endpoint='/admin/mail/users?format=json',
    #     dag=dag)

    # def mailExists_func(ti):
    #     xcom_value = str(ti.xcom_pull(task_ids="mail_checkMbox"))
    #     json_xcom = json.loads(xcom_value)

    #     if len(json_xcom) > 0:
    #         return "mail_createMbox"
    #     else:
    #         return "do_nothing"

    # mail_Exists = BranchPythonOperator(
    #     task_id="mail_Exists",
    #     python_callable=mailExists_func,
    #     # trigger_rule='always',
    #     dag=dag,
    # )

    mail_createMbox = SimpleHttpOperator(
        http_conn_id='mailbox_connection',
        task_id='mail_createMbox',
        method='POST',
        endpoint='/admin/mail/users/add',
        data=f'email={username}@{domain}&password={password}',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        # response_check=lambda response: True if  (response == "mail user added" or response == "User already exists.")  else False,s
        dag=dag)

    email_notify = EmailOperator(
        task_id='email_notify',
        to=mail_to,
        subject=f'Airflow: {username} created',
        html_content=f'Student: <h3>{realEmail}</h3><br/>User:<h3> {username}</h3><br/> password: <h3>{password}</h3>',
        trigger_rule='none_skipped',
        dag=dag
    )
    email_notify_user = EmailOperator(
        task_id='email_notify_user',
        to=f'{realEmail},{mail_to}',
        subject=f'Welcome to {labName}',
        html_content=f'You have been enrolled into {labName} with the following credentials<br/>User:<h3> {username}</h3><br/> password: <h3>{password}</h3>',
        trigger_rule='none_skipped',
        dag=dag
    )

sensor >> kc_token >> kc_checkUser >> kc_userExists >> [
    kc_createUser, do_nothing]
kc_createUser >> email_notify >> email_notify_user,
domain_check >> [mail_createMbox, do_nothing]
kc_createUser >> mail_createMbox
mail_createMbox >> email_notify
