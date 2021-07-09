import json
import random
import string
from datetime import timedelta

import airflow
import logging
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.models import XCom
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.trigger_rule import TriggerRule


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
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

code = "{{ dag_run.conf['code'] }}"
rEmail = "{{ dag_run.conf['realEmail'] }}"
labName = "{{ dag_run.conf['labName'] }}"


resource = "{{ dag_run.conf['resource'] }}"
email = "{{ dag_run.conf['email'] }}"
username = "{{ dag_run.conf['username'] }}"
password = ''.join(random.choice(string.ascii_uppercase +
                   string.digits) for _ in range(11))  # snipe-it requires 10 char passw
expiration = "{{ dag_run.conf['expiration'] }}"

userData = {
    'first_name': username,
    'email': email,
    'username': username,
    'password': password,
    'password_confirmation': password
}


with DAG('snipeit_provision',
         schedule_interval=None,
         default_args=default_args,
         tags=['swarm', 'esxi', 'validation', 'email', 'snipe'],
         catchup=False,
         user_defined_filters={'fromjson': lambda s: json.loads(s)},
         max_active_runs=1
         ) as dag:

    snipe_category = SimpleHttpOperator(
        http_conn_id='snipe-it',
        task_id='snipe_category',
        method='GET',
        endpoint=f'/api/v1/categories?limit=1&search={resource}',
        # response_check=lambda response: True if 'access_token' in response.json() else False,
        log_response=True,
        dag=dag)

    snipe_hardware_list = SimpleHttpOperator(
        http_conn_id='snipe-it',
        task_id='snipe_hardware_list',
        method='GET',
        endpoint='/api/v1/hardware?&status=RTD&limit=1&category_id={{ (task_instance.xcom_pull(key="return_value", task_ids="snipe_category")| fromjson)["rows"][0]["id"] }}',
        # data='client_id='+kc_client+'&grant_type=' +
        # 'client_credentials'+'&client_secret='+kc_secret,
        # headers={"Content-Type": "application/x-www-form-urlencoded"},
        # response_check=lambda response: True if 'access_token' in response.json() else False,
        log_response=True,
        dag=dag)

    snipe_check_user = SimpleHttpOperator(
        http_conn_id='snipe-it',
        task_id='snipe_check_user',
        method='GET',
        endpoint=f'/api/v1/users?username={username}&email={email}',
        # response_check=lambda response: True if int(
        #     response.json()['total']) == 1 else False,
        log_response=True,
        dag=dag)

    def count_users(**context):
        json_raw = context['ti'].xcom_pull(task_ids='snipe_check_user')
        json_result = json.loads(json_raw)
        result = int(json_result['total'])
        if (result == 0):
            return "snipe_create_user"
        else:
            return "do_nothing"

    do_nothing = DummyOperator(task_id='do_nothing', dag=dag)

    branch_user_create = BranchPythonOperator(
        task_id='branch_user_create',
        python_callable=count_users,
        dag=dag)

    snipe_create_user = SimpleHttpOperator(
        http_conn_id='snipe-it',
        task_id='snipe_create_user',
        method='POST',
        endpoint=f'/api/v1/users',
        data=json.dumps(userData),
        log_response=True,
        headers={"Accept": "application/json"},
        response_check=lambda response: True if response.json()[
            'status'] != 'error' else False,
        # trigger_rule='one_failed',
        # depends_on_past=True,
        dag=dag)

    # @dag.task(multiple_outputs=True)
    def checkout_body(**context):
        ti = context['ti']
        conf = context['dag_run'].conf
        # user_id from either list or creation
        string_result = ti.xcom_pull(
            task_ids='snipe_check_user', key='return_value')
        json_result = json.loads(string_result)
        if(json_result['total'] == 1):
            user_id = json_result['rows'][0]['id']
        else:
            string_result = ti.xcom_pull(
                task_ids='snipe_create_user', key='return_value')
            json_result = json.loads(string_result)
            user_id = json_result['payload']['id']
        # hardware
        asset_raw = ti.xcom_pull(
            task_ids='snipe_hardware_list', key='return_value')
        asset_json = json.loads(asset_raw)["rows"][0]

        body = {
            'endpoint': f'/api/v1/hardware/{asset_json["id"]}/checkout',
            'checkout_to_type': 'user',
            'assigned_user': user_id,
            'expected_checkin': conf.get('expiration'),
            'note': asset_json["notes"],
        }
        return json.dumps(body)

    prepare_checkout = PythonOperator(
        task_id='prepare_checkout',
        python_callable=checkout_body,
        trigger_rule='one_success',
        dag=dag,
    )

    snipe_checkout_asset = SimpleHttpOperator(
        http_conn_id='snipe-it',
        task_id='snipe_checkout_asset',
        method='POST',
        endpoint='{{(task_instance.xcom_pull(key="return_value", task_ids="prepare_checkout")| fromjson)["endpoint"]}}',
        data="{{task_instance.xcom_pull(key='return_value', task_ids='prepare_checkout')}}",
        log_response=True,
        headers={"Accept": "application/json"},
        response_check=lambda response: True if response.json()[
            'status'] != 'error' else False,
        dag=dag)

    def sql_body(**context):
        ti = context['ti']
        conf = context['dag_run'].conf
        asset_raw = ti.xcom_pull(
            task_ids='snipe_hardware_list', key='return_value')
        asset_json = json.loads(asset_raw)["rows"][0]

        uservar = json.dumps({
            'name':  conf.get('resource'),
            'value': asset_json['name']
        })
        return f"""UPDATE `Users` 
            SET `variables` = JSON_ARRAY_APPEND(`variables`, '$', CAST('{uservar}' as JSON))
            WHERE `Users`.`username` = '{conf.get('username')}';"""

    prepare_sql = PythonOperator(
        task_id='prepare_sql',
        python_callable=sql_body,
        trigger_rule='one_success',
        dag=dag,
    )

    mysql_user_variable = MySqlOperator(
        sql="{{task_instance.xcom_pull(key='return_value', task_ids='prepare_sql')}}",
        mysql_conn_id='validation_db',
        task_id='mysql_user_variable',
        trigger_rule='none_failed',
        dag=dag)

snipe_category >> snipe_hardware_list >> prepare_checkout
snipe_check_user >> branch_user_create >> [
    do_nothing, snipe_create_user] >> prepare_checkout >> snipe_checkout_asset >> prepare_sql >> mysql_user_variable
