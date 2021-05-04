import json
from datetime import timedelta

import airflow
import logging
import hashlib
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator


mail_err = Variable.get('MAIL_ERR')

lvs_attempt_id = "{{ dag_run.conf['validationAttempt_Id'] }}"
lvs_test = "{{ dag_run.conf['test'] }}"
lvs_script = '{{ dag_run.conf["script"] }}'
lvs_val_score = '{{ dag_run.conf["score"] }}'
lvs_eaa_user = '{{ dag_run.conf["eaa_user"] }}'
lvs_etp_user = '{{ dag_run.conf["etp_user"] }}'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': True,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': [mail_err],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG('lvs_validation',
         schedule_interval=None,
         default_args=default_args,
         tags=['lvs', 'validation'],
         catchup=False,
         user_defined_filters={'fromjson': lambda s: json.loads(s)}
         ) as dag:

    def branch_func(**kwargs):
        return kwargs['dag_run'].conf.get('test')

    branch_op = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_func,
        dag=dag)

    mysql_mark_completed = MySqlOperator(
        sql=f"""UPDATE `ValidationAttempts` SET `completed` = '1' WHERE `ValidationAttempts`.`id` = '{lvs_attempt_id}';""",
        mysql_conn_id='validation_db',
        task_id='mysql_mark_completed',
        parameters={"id": lvs_attempt_id},
        trigger_rule='all_done',
        dag=dag)

    mysql_mark_passed = MySqlOperator(
        sql=f"""UPDATE `ValidationAttempts` 
            SET `score` = {lvs_val_score}
            , `feedback` = JSON_ARRAY_APPEND(`feedback`, '$', 'Validation succeded')
            WHERE `ValidationAttempts`.`id` = '{lvs_attempt_id}';""",
        mysql_conn_id='validation_db',
        task_id='mysql_mark_passed',
        parameters={"id": lvs_attempt_id},
        trigger_rule='none_failed',
        dag=dag)

    mysql_mark_failed = MySqlOperator(
        sql=f"""UPDATE `ValidationAttempts` 
            SET `score` =  0 
            , `feedback` = JSON_ARRAY_APPEND(`feedback`, '$', 'Validation failed')
            WHERE `ValidationAttempts`.`id` = '{lvs_attempt_id}';""",
        mysql_conn_id='validation_db',
        task_id='mysql_mark_failed',
        parameters={"id": lvs_attempt_id},
        trigger_rule='one_failed',
        dag=dag)

    dummyPass = DummyOperator(task_id='dummyPass', dag=dag)

    def python_dummy2():
        raise AirflowFailException("Validation Failed")
    dummyFail = PythonOperator(
        dag=dag,
        task_id='dummyFail',
        python_callable=python_dummy2
    )
    eaa_test = SSHOperator(
        task_id='eaa_test',
        command=f'pytest-3 /home/airflow/tests/eaa/{lvs_eaa_user}/{lvs_script}',
        ssh_conn_id='selenium',
        dag=dag,
        retries=2,
    )
    etp_test = SSHOperator(
        task_id='etp_test',
        command=f'pytest-3 /home/airflow/tests/etp/{lvs_etp_user}/{lvs_script}',
        ssh_conn_id='selenium',
        dag=dag,
        retries=2,
    )

    branch_op >> [dummyPass, dummyFail,
                  eaa_test, etp_test] >> mysql_mark_passed
    branch_op >> [dummyPass, dummyFail,
                  eaa_test, etp_test] >> mysql_mark_failed
    [mysql_mark_passed, mysql_mark_failed] >> mysql_mark_completed
