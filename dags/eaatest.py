import json
from datetime import timedelta

import airflow
import logging
import hashlib 
import datetime
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule




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




username="{{ dag_run.conf['username'] }}"
eaa_user="{{ dag_run.conf['eaa_user'] }}"
#username='testairflow'
#eaa_user='user50'

# ClassMarker Data
cm_api_key=Variable.get('CM_API_KEY')
cm_api_sec=Variable.get('CM_API_SECRET')
cm_list=Variable.get('CM_LIST_ETP')
timestamp=int(datetime.datetime.utcnow().timestamp())
concat=f'{cm_api_key}{cm_api_sec}{timestamp}'
signature=hashlib.md5(concat.encode()).hexdigest()
post_body=[]
post_body.append(username)

dag = DAG('eaa_test',
        schedule_interval=None, 
        default_args=default_args,
        tags=['classmarker'],
        catchup=False,
        user_defined_filters={'fromjson': lambda s: json.loads(s)}
        )

test_finance = SSHOperator(
    task_id='test_finance',
    command=f'pytest-3 /home/airflow/tests/eaa/{eaa_user}/test_finance.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

test_intranet = SSHOperator(
    task_id='test_intranet',
    command=f'pytest-3 /home/airflow/tests/eaa/{eaa_user}/test_intranet.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

test_no_finance = SSHOperator(
    task_id='test_no_finance',
    command=f'pytest-3 /home/airflow/tests/eaa/{eaa_user}/test_no_finance.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

test_no_intranet = SSHOperator(
    task_id='test_no_intranet',
    command=f'pytest-3 /home/airflow/tests/eaa/{eaa_user}/test_no_intranet.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

test_status = SSHOperator(
    task_id='test_status',
    command=f'pytest-3 /home/airflow/tests/eaa/{eaa_user}/test_status.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

cm_enrollment = SimpleHttpOperator(
    http_conn_id='classmarker_api',
    task_id='cm_enrollment',
    method='POST',
    endpoint=f'/v1/accesslists/{cm_list}.json?api_key={cm_api_key}&signature={signature}&timestamp={timestamp}',
    data=json.dumps(post_body),
    headers={"Content-Type": "application/json"},
    response_check=lambda response: True if ('status' in response.json() and response.json()['status']=='ok') else False,
    xcom_push=True,
    log_response=True,
    dag=dag)


email_failure = EmailOperator(
        task_id='email_failure',
        to=f'amesegue@akamai.com,{username}@akamaipartnertraining.com,partnertraining@akamai.com',
        subject=f'ETP certification',
        html_content=f'We regret to inform you that you have not passed your attempt of the Practical component of the Akamai Advanced Partner ETP Certification. \
            Take your time, study the areas you had trouble with, and when you are ready, please attempt again!',
        trigger_rule='one_failed',
        dag=dag
)

email_success = EmailOperator(
        task_id='email_success',
        to=f'amesegue@akamai.com,{username}@akamaipartnertraining.com,partnertraining@akamai.com',
        subject=f'Certification approval: {username} has completed ETP certification',
        html_content=f'<h4>Congratulations!<h4>  You have successfully passed the practical examination component of the Akamai Advanced Partner ETP Certification.\
              In order to complete your certification, you must also successfully complete the accompanying multiple choice exam component.\
              Please go to your preferred link below and use your username {username} as access code. Enter your own email address when prompted for it.\
            <ul>\
                <li><a  target="_blank" href="https://www.classmarker.com/online-test/start/?quiz=qtd5f9e9520d4ea9">English</a></li>\
            <ul>\
            <h4>If you require any assistance, please contact <a href="mailto:partnertraining@akamai.com">partnertraining@akamai.com</a> for help.</h4>',
        dag=dag
)


test_status >> test_intranet >> test_no_intranet
test_status >> test_finance >> test_no_finance

test_no_finance >> email_failure 
test_no_intranet >> email_failure

test_no_finance >>  cm_enrollment >> email_success
test_no_intranet >>  cm_enrollment >> email_success
