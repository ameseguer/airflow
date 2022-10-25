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
from airflow.decorators import  task


kc_secret = Variable.get('KEYCLOAK_SECRET')
kc_realm = Variable.get('KEYCLOAK_REALM')
kc_client = Variable.get('KEYCLOAK_CLIENT')

chimp_key = Variable.get('MAILCHIMP_KEY')

domain = Variable.get('DOMAIN')
prefix = Variable.get('USER_PREFIX')
mail_to = Variable.get('MAIL_NOTIFY_TO')
mail_err = Variable.get('MAIL_ERR')
mail_create = Variable.get('MAIL_ENROLLMENT_NOTIFY')


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
    'params': {
        "template":"startpartner",
        "labName": "Akamai Certification",
        "realEmail": "NA",
        "password": "[NO CHANGE ... Keep using your current password]",
        "username": "NA",
        "firstName": "student",
        "lastName": "NA",
        "startDate": "NA",
        "endDate": "NA",
        "labId": "NA",
        "enroller": "NA",
        "eaa_user": "NA",
        "eaa_password": "NA",
        "etp_user": "NA",
        "etp_password": "NA",
        "userzone": "NA",
        "dnsletter": "c",
        "dns1": "NA",
        "dns2": "NA",
        "enterprisenumber":"00",
        "createdBy": mail_err
    }
}

chimpData ={
    'key': chimp_key,
    'template_name': '{{params.template}}' ,
    'template_content': [{}],
    'message':{
        "merge": True,
        "preserve_recipients": True,
        "global_merge_vars": [{
                "name": "login",
                "content": "{{params.username}}"
        }, {
                "name": "password",
                "content": "{{params.password}}"
        }, {
                "name": "lab",
                "content": "{{params.labName}}"
        }, {
                "name": "startdate",
                "content": "{{params.startDate}}"
        }, {
                "name": "enddate",
                "content": "{{params.endDate}}"
        }, {
                "name": "labid",
                "content": "{{params.labId}}"
        }, {
                "name": "enroller",
                "content": "{{params.enroller}}"
        }, {
                "name": "lastname",
                "content": "{{params.lastName}}"
        }, {
                "name": "eaa_user",
                "content": "{{params.eaa_user}}"
        }, {
                "name": "etp_user",
                "content": "{{params.etp_user}}"
        }, {
                "name": "dns1",
                "content": "{{params.dns1}}"
        }, {
                "name": "dns2",
                "content": "{{params.dns2}}"
        }, {
                "name": "dnsletter",
                "content": "{{params.dnsletter}}"
        }, {
                "name": "etp_password",
                "content": "{{params.etp_password}}"
        }, {
                "name": "eaa_password",
                "content": "{{params.eaa_password}}"
        }, {
                "name": "userzone",
                "content": "{{params.userzone}}"
        }, {
                "name": "enterprisenumber",
                "content": "{{params.enterprisenumber}}"
        }, {
                "name": "firstname",
                "content": "{{params.firstName}}"
        }],
        "to": [{
                "email": "{{params.realEmail}}",
                "type": "to"
            },{
                "email": "{{params.createdBy}}",
                "type": "cc"
            }]
        }
    }
#add the different admins
mail_tos = mail_to.split(',')
for mail_address in mail_tos:
    chimpData['message']['to'].append({"email":mail_address,"type":"bcc"})
mail_tos="{{params.createdBy}}"

with DAG('lvs_enrollment_notify',
         schedule_interval=None,
         default_args=default_args,
         tags=['enrollment', 'validation',  'email'],
         catchup=False,
         user_defined_filters={'fromjson': lambda s: json.loads(s)},
         max_active_runs=1
         ) as dag:


    def chimp_var(**kwargs):
        if chimp_notify == "False":
            return 'do_nothing'
        else:
            return 'mail_createMbox'

    #do_nothing = DummyOperator(task_id='do_nothing', dag=dag)

    #chimp_check = BranchPythonOperator(
    #    task_id='chimp_check',
    #    python_callable=chimp_var,
    #    dag=dag
    #)

    email_notify_user = SimpleHttpOperator(
        http_conn_id='mailchimp_api',
        task_id='email_notify_user',
        method='POST',
        endpoint='/api/1.0/messages/send-template',
        data=json.dumps(chimpData),
        headers={'Content-Type': 'application/json'},
        dag=dag)

email_notify_user
