from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Olega',
    'start_date': days_ago(0),
    'email': 'olegpa2005@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay':timedelta(minutes=3),
    }

dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args= default_args,
    description= 'I make pipeline by myself',
    schedule_interval=timedelta(days=1),
    )

download = BashOperator(
    task_id = 'download',
    bash_command = 'wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"',
    dag = dag,
    )

extract = BashOperator(
    task_id = 'extract',
    bash_command = 'cut -f1,4 -d"#" web-server-access-log.txt > /home/airflow/dags/extracted.txt',
    dag=dag,
    )

transform = BashOperator(
    task_id = 'transform',
    bash_command = 'tr "[a-z]" "[A-Z]" < /home/airflow/dags/extracted.txt > /home/airflow/dags/capitalized.txt',
    dag=dag,
    )

load = BashOperator(
    task_id = 'load',
    bash_command='zip log.zip capitalized.txt',
    dag=dag,
    )

download >> extract >> transform >> load