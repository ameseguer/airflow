import json
from datetime import timedelta

import airflow
import logging
import base64
from airflow import DAG
from airflow.decorators import task
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable



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

with DAG('ldap_posix_account',
        schedule_interval='0 */5 * * *', 
        default_args=default_args,
        tags=['ldap'],
        catchup=False,
        ) as dag:

    ldap_posix_account = SSHOperator(
        task_id='ldap_posix_account',
        command='/home/airflow/shell-server/ldapPosixAccount.sh {{ var.value.LDAP_BASE }}  {{ var.value.LDAP_BIND }} {{ var.value.LDAP_PASSWORD }} {{ var.value.LDAP_UID }}',
        ssh_conn_id='shell-server',
        dag=dag,
        retries=0,
        do_xcom_push=True
        )

    def count_newlines(**context):
        string_result=context['ti'].xcom_pull(task_ids='ldap_posix_account')
        decoded=base64.b64decode(string_result)
        decoded_string=str(decoded)
        result=decoded_string.count('\\n')
        LDAP_UID=int(Variable.get('LDAP_UID'))
        if (result > 0) :
            LDAP_UID+=result
            Variable.set('LDAP_UID',LDAP_UID)
            return {"posix_accounts_fixed": result, 'LDAP_UID':LDAP_UID}
        else:
            return {"posix_accounts_fixed": 0,'LDAP_UID':LDAP_UID}


    ldap_result = PythonOperator(
        task_id='ldap_result',
        python_callable=count_newlines,
        dag=dag,
    )


    ldap_posix_account >> ldap_result
