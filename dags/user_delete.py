import json
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


kc_secret = Variable.get('KEYCLOAK_SECRET')
kc_realm = Variable.get('KEYCLOAK_REALM')
kc_client = Variable.get('KEYCLOAK_CLIENT')

mail_admin = Variable.get('MAIL_ADMIN')
mail_passw = Variable.get('MAIL_ADMIN_PASS')

domain = Variable.get('DOMAIN')
prefix = Variable.get('USER_PREFIX')

mail_to = Variable.get('MAIL_NOTIFY_TO')
mail_create = Variable.get('MAIL_CREATE')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': [mail_to],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'params': {
        "username": "NA",
        "domain": domain
    }
}

with DAG('user_delete',
         schedule_interval=None,
         default_args=default_args,
         tags=['user_delete', 'validation', 'keycloak', 'email'],
         catchup=False,
         max_active_runs=1,
         user_defined_filters={'fromjson': lambda s: json.loads(s)}
         ) as dag:

    dag.doc_md = __doc__

    code = '{{ dag_run.conf["code"] if dag_run.conf["code"] is defined else "" }}'
    username = '{{ dag_run.conf["username"] if dag_run.conf["username"] is defined else "" }}'

    if(username == ""):
        username = f'{prefix}{code}'

    sensor = HttpSensor(
        task_id='http_kc_check',
        http_conn_id='kc_connection',
        endpoint='',
        request_params={},
        response_check=lambda response: True if len(
            response.content) >= 0 else False,
        poke_interval=5,
        dag=dag)

    kc_token = SimpleHttpOperator(
        http_conn_id='kc_connection',
        task_id='kc_token',
        method='POST',
        endpoint=f'/auth/realms/{kc_realm}/protocol/openid-connect/token',
        data='client_id='+kc_client+'&grant_type=' +
        'client_credentials'+'&client_secret='+kc_secret,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        response_check=lambda response: True if 'access_token' in response.json() else False,
        log_response=True,
        dag=dag)

    kc_get_user_data = SimpleHttpOperator(
        http_conn_id='kc_connection',
        task_id='kc_get_user_data',
        method='GET',
        endpoint=f'/auth/admin/realms/{kc_realm}/users',
        data='username={{params.username}}',
        headers={'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Bearer ' +
                 '{{(task_instance.xcom_pull(key="return_value", task_ids="kc_token")| fromjson)["access_token"] }}'},
        response_check=lambda response: True if response.status_code < 400 else False,
        dag=dag)

    def userExists_func(ti):
        xcom_value = ti.xcom_pull(task_ids="kc_get_user_data")
        print(f"value is {xcom_value}")
        if xcom_value is None:
            return "do_nothing"
        else:
            json_xcom = json.loads(str(xcom_value))
            if len(json_xcom) == 0:
                return "do_nothing"
            else:
                return "kc_delete_user"

    kc_userExists = BranchPythonOperator(
        task_id="kc_userExists",
        python_callable=userExists_func,
        trigger_rule='all_done',
        dag=dag,
    )
    kc_delete_user = SimpleHttpOperator(
        http_conn_id='kc_connection',
        task_id='kc_delete_user',
        method='DELETE',
        endpoint=f'/auth/admin/realms/{kc_realm}/users/' +
        '{{(task_instance.xcom_pull(key="return_value", task_ids="kc_get_user_data")| fromjson)[0]["id"] }}',
        headers={'Authorization': 'Bearer ' +
                 '{{(task_instance.xcom_pull(key="return_value", task_ids="kc_token")| fromjson)["access_token"] }}'},
        response_check=lambda response: True if response.status_code < 400 else False,
        dag=dag)

    def aka_mail(**kwargs):
        if mail_create == "False":
            return 'do_nothing'
        else:
            return 'mail_delete'

    do_nothing = DummyOperator(task_id='do_nothing', dag=dag)

    domain_check = BranchPythonOperator(
        task_id='domain_check',
        python_callable=aka_mail,
        dag=dag
    )

    mail_delete = SimpleHttpOperator(
        http_conn_id='mailbox_connection',
        task_id='mail_delete',
        method='POST',
        endpoint='/admin/mail/users/remove',
        data='email={{(task_instance.xcom_pull(key="return_value", task_ids="kc_get_user_data")| fromjson)[0]["email"] }}',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        response_check=lambda response: True if response.status_code < 400 else False,
        dag=dag)

    email_notify = EmailOperator(
        task_id='email_notify',
        to=mail_to,
        subject='Airflow: User {{params.username}} deleted',
        html_content='<h3>User {{params.username}} has been deleted</h3>',
        dag=dag
    )

    sensor >> kc_token >> kc_get_user_data >> kc_userExists >> [
        kc_delete_user, do_nothing]
    kc_delete_user >> email_notify
    kc_delete_user >> domain_check >> [mail_delete, do_nothing]
