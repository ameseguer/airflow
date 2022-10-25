import airflow
from time import sleep
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.ssh_operator import SSHOperator


domain = Variable.get('DOMAIN')
prefix = Variable.get('USER_PREFIX')
mail_to = Variable.get('MAIL_NOTIFY_TO')
mail_err = Variable.get('MAIL_ERR')
mail_create = Variable.get('MAIL_CREATE')
esxi_path = Variable.get('ESXI_PATH')


scripts=[
'reset_seat_enterprise.sh',
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': [mail_err],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'params': {
        "labName": "NA",
        "realEmail": "NA",
        "username": "NA",
        "firstName": "student",
        "lastName": "NA",
        "startDate": "NA",
        "endDate": "NA",
        "labId": "NA",
        "enterprisenumber": "00",
        "createdBy": mail_err
    }
}


with DAG('esxi_provision',
         schedule_interval=None,
         default_args=default_args,
         tags=['esxi_provision', 'validation', 'esxi', 'eaa', 'etp','mfa'],
         catchup=False,
         max_active_runs=1
         ) as dag:
    
    tasks = []
    
    for task_n in range(len(scripts)):
      step = SSHOperator(
          task_id=scripts[task_n],
          command= "{{ var.value.get('ESXI_PATH', '~/') }}/"+str(scripts[task_n])+" {{params.enterprisenumber}}",
          ssh_conn_id='esxi_control',
          dag=dag,
          retries=0,
      )
      if task_n == 0 :
        step.post_execute = lambda **x: sleep(300)#sleep 5m after reboot
      tasks.append(step)
    
for task_n in range(len(tasks)):
    if task_n < len(tasks)-1:
        tasks[task_n] >> tasks[task_n+1]
