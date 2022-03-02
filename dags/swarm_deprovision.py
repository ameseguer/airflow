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


swarm_auth = {
    'Username': f'{swarm_user}',
    'Password': f'{swarm_password}',

}

with DAG('swarm_deprovision',
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

    # swarm_search = SimpleHttpOperator(
    #     http_conn_id='docker_swarm',
    #     task_id='swarm_search',
    #     method='GET',
    #     endpoint='/api/stacks',
    #     headers={'Authorization': 'Bearer ' +
    #              '{{ (task_instance.xcom_pull(key="return_value", task_ids="swarm_token")| fromjson)["jwt"] }}'},
    #     log_response=True,
    #     dag=dag)

    # def python_find_stack(**context):
    #     json_raw = context['ti'].xcom_pull(task_ids='swarm_search')
    #     json_result = json.loads(json_raw)

    #     resource = context['dag_run'].conf.get('resource')
    #     code = context['dag_run'].conf.get('code')
    #     targetName = f'{resource}-{code}'

    #     for stack in json_result:
    #         if stack['Name'] == targetName:
    #             print(stack)
    #             return stack['Id']
    #     return "null"

    # find_stack = PythonOperator(
    #     task_id='find_stack',
    #     python_callable=python_find_stack,
    #     dag=dag)

    swarm_delete = SimpleHttpOperator(
        http_conn_id='docker_swarm',
        task_id='swarm_delete',
        method='DELETE',
        endpoint='/api/stacks/{{dag_run.conf["providerId"] }}',
        headers={'Authorization': 'Bearer ' +
                 '{{ (task_instance.xcom_pull(key="return_value", task_ids="find_stack")| fromjson)["jwt"] }}'},
        log_response=True,
        dag=dag)

    mysql_mark_deprovisioned = MySqlOperator(
        sql="""UPDATE `Assets` SET `deprovisioned` = '1' ,`procesing`='0' WHERE `Assets`.`id` = '{{ dag_run.conf["assetId"] }}';""",
        mysql_conn_id='validation_db',
        task_id='mysql_mark_deprovisioned',
        trigger_rule='all_done',
        dag=dag)


swarm_token >> swarm_delete >> mysql_mark_deprovisioned
