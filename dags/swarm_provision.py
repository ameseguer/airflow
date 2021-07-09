import json
from datetime import timedelta

import airflow
import logging
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


swarm_user = Variable.get('SWARM_USER')
swarm_password = Variable.get('SWARM_PASSWORD')
swarm_id = Variable.get('SWARM_ID')

mail_err = Variable.get('MAIL_ERR')


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

code = "{{ dag_run.conf['code'] }}"
resource = "{{ dag_run.conf['resource'] }}"

swarm_data = {
    'Name': f'{resource}-{code}',
    'SwarmID': f'{swarm_id}',
    'StackFileContent': f'{resource}-{code}',
    'RepositoryURL': 'https://github.com/ameseguer/portainer-compose/',
    'RepositoryReferenceName': 'refs/heads/main',
    'ComposeFilePathInRepository': f'origins/{resource}.yml',
    'RepositoryAuthentication': False,
    'Env': [{
        'name': 'PREFIX',
        'value': f'{code}'
    }]
}

swarm_auth = {
    'Username': f'{swarm_user}',
    'Password': f'{swarm_password}',

}

with DAG('swarm_provision',
         schedule_interval=None,
         default_args=default_args,
         tags=['swarm', 'user_create'],
         catchup=False,
         user_defined_filters={'fromjson': lambda s: json.loads(s)}
         ) as dag:

    swarm_token = SimpleHttpOperator(
        http_conn_id='docker_swarm',
        task_id='swarm_token',
        method='POST',
        endpoint=f'/api/auth',
        data=json.dumps(swarm_auth),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: True if 'jwt' in response.json() else False,
        log_response=True,
        dag=dag)

    swarm_create = SimpleHttpOperator(
        http_conn_id='docker_swarm',
        task_id='swarm_create',
        method='POST',
        endpoint='/api/stacks?type=1&method=repository&endpointId=1',
        data=json.dumps(swarm_data),
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' +
                 '{{ (task_instance.xcom_pull(key="return_value", task_ids="swarm_token")| fromjson)["jwt"] }}'},
        response_check=lambda response: True if (
            response.status_code < 400 or response.status_code == 409) else False,
        log_response=True,
        dag=dag)


swarm_token >> swarm_create
