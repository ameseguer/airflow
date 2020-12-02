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
etp_user="{{ dag_run.conf['etp_user'] }}"

# ClassMarker Data
cm_api_key=Variable.get('CM_API_KEY')
cm_api_sec=Variable.get('CM_API_SECRET')
cm_list=Variable.get('CM_LIST_ETP')
timestamp=int(datetime.datetime.utcnow().timestamp())
concat=f'{cm_api_key}{cm_api_sec}{timestamp}'
signature=hashlib.md5(concat.encode()).hexdigest()
post_body=[]
post_body.append(username)

dag = DAG('etp_test',
        schedule_interval=None, 
        default_args=default_args,
        tags=['classmarker'],
        catchup=False,
        )

test_amazon = SSHOperator(
    task_id='test_amazon.',
    command=f'pytest-3 /home/airflow/tests/etp/{etp_user}/test_amazon.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

test_eicar = SSHOperator(
    task_id='test_intranet',
    command=f'pytest-3 /home/airflow/tests/etp/{etp_user}/test_eicar.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

test_lingerie = SSHOperator(
    task_id='test_no_finance',
    command=f'pytest-3 /home/airflow/tests/etp/{etp_user}/test_lingerie.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

test_proxy1 = SSHOperator(
    task_id='test_no_intranet',
    command=f'pytest-3 /home/airflow/tests/etp/{etp_user}/test_proxy1.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

test_proxy2 = SSHOperator(
    task_id='test_proxy2',
    command=f'pytest-3 /home/airflow/tests/etp/{etp_user}/test_proxy2.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

test_reddit = SSHOperator(
    task_id='test_reddit',
    command=f'pytest-3 /home/airflow/tests/etp/{etp_user}/test_reddit.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )

test_social = SSHOperator(
    task_id='test_social',
    command=f'pytest-3 /home/airflow/tests/etp/{etp_user}/test_social.py',
    ssh_conn_id='selenium',
    dag=dag,
    retries=0,
    do_xcom_push=True
    )   

test_weapons = SSHOperator(
    task_id='test_weapons',
    command=f'pytest-3 /home/airflow/tests/etp/{etp_user}/test_weapons.py',
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
                <li><a  target="_blank" href="https://www.classmarker.com/online-test/start/?quiz=fqf5fa552808ec52">English</a></li>\
            <ul>\
            <h4>If you require any assistance, please contact <a href="mailto:partnertraining@akamai.com">partnertraining@akamai.com</a> for help.</h4>',
        dag=dag
)


test_amazon >> cm_enrollment >> email_success
test_eicar >> cm_enrollment >> email_success
test_lingerie >> cm_enrollment >> email_success
test_proxy1 >> cm_enrollment >> email_success
test_proxy2 >> cm_enrollment >> email_success
test_reddit >> cm_enrollment >> email_success
test_social >> cm_enrollment >> email_success
test_weapons >> cm_enrollment >> email_success

test_amazon >> email_failure
test_eicar >> email_failure
test_lingerie >> email_failure
test_proxy1 >> email_failure
test_proxy2 >> email_failure
test_reddit >> email_failure
test_social >> email_failure
test_weapons >> email_failure
