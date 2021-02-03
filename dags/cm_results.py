import json
from datetime import timedelta

import airflow
import logging
import hashlib 
import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
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

with DAG(
    'cm_results', 
    default_args=default_args,
    schedule_interval='0 */6 * * *',
    tags=['ClassMarker','partners']
) as dag:

    time_range=int(Variable.get('CM_RESULTS_TIMESTAMP'))
    cm_api_key=Variable.get('CM_API_KEY')
    cm_api_sec=Variable.get('CM_API_SECRET')
    timestamp=int(datetime.datetime.utcnow().timestamp())
    concat=f'{cm_api_key}{cm_api_sec}{timestamp}'
    signature=hashlib.md5(concat.encode()).hexdigest()


    cm_results = SimpleHttpOperator(
        http_conn_id='classmarker_api',
        task_id='cm_results',
        method='GET',
        endpoint=f'/v1/links/recent_results.json?api_key={cm_api_key}&signature={signature}&timestamp={timestamp}&finishedAfterTimestamp={time_range}',
        response_check=lambda response: True if ('status' in response.json()) else False,
        log_response=True,
        dag=dag)

    def sequelize(l):
        r='('
        first=True
        for i in l:
            if(not first):
                r+=','
            else:
                first=False
            if(isinstance(i,int)):
                r+=str(i)
            else:
                r+=f"'{i}'"
        r+=')'
        return r

    def python_parse_results(**kwargs):
        ti = kwargs['ti']
        resultsJson = ti.xcom_pull(task_ids='cm_results')
        results=json.loads(resultsJson)

        if (results['status'] != 'ok'):
            return 'no_results'
        if(results['next_finished_after_timestamp']):
            Variable.set("CM_RESULTS_TIMESTAMP", results['next_finished_after_timestamp'])
        sql_links='INSERT IGNORE INTO classmarker.links('
        value_keys=False
        first=True
        for res in results['links']:
            link=dict(sorted(res['link'].items()))
            if (not value_keys) :
                sql_links+=','.join(link.keys())
                sql_links+=') VALUES'
                value_keys=True
            if(first):
                first=False
            else:
                sql_links+=','
            sql_links+=sequelize(link.values())
        sql_links+=';'
        
        sql_tests='INSERT IGNORE INTO classmarker.tests('
        value_keys=False
        first=True
        for res in results['tests']:
            test=dict(sorted(res['test'].items()))
            if (not value_keys) :
                sql_tests+=','.join(test.keys())
                sql_tests+=') VALUES'
                value_keys=True
            if(first):
                first=False
            else:
                sql_tests+=','
            sql_tests+=sequelize(test.values())
        sql_tests+=';'

        sql_results='INSERT IGNORE INTO classmarker.results('
        value_keys=False
        first=True
        for res in results['results']:
            result=dict(sorted(res['result'].items()))
            if (not value_keys) :
                sql_results+=','.join(result.keys())
                sql_results+=') VALUES'
                value_keys=True
            if(first):
                first=False
            else:
                sql_results+=','
            sql_results+=sequelize(result.values())      
        sql_results+=';'
        
        ti.xcom_push(key='sql_links', value=sql_links)
        ti.xcom_push(key='sql_tests', value=sql_tests)
        ti.xcom_push(key='sql_results', value=sql_results)

        return 'insert_mysql'

    parse_results=BranchPythonOperator(
        task_id='parse_results',
        provide_context=True,
        python_callable=python_parse_results,
        dag=dag)

    no_results = DummyOperator(task_id='no_results', dag=dag)

    insert_mysql = DummyOperator(task_id='insert_mysql', dag=dag)

    mysql_links = MySqlOperator(
            sql='{{task_instance.xcom_pull(key="sql_links", task_ids="parse_results")}}',
            mysql_conn_id = 'classmarker_db',
            task_id='mysql_links',
            dag=dag)

    mysql_tests = MySqlOperator(
            sql='{{task_instance.xcom_pull(key="sql_tests", task_ids="parse_results")}}',
            mysql_conn_id = 'classmarker_db',
            task_id='mysql_tests',
            dag=dag)
    mysql_results = MySqlOperator(
            sql='{{task_instance.xcom_pull(key="sql_results", task_ids="parse_results")}}',
            mysql_conn_id = 'classmarker_db',
            task_id='mysql_results',
            dag=dag)

cm_results >> parse_results >>[insert_mysql,no_results]
insert_mysql >> [mysql_links,mysql_tests,mysql_results] 

