from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Olega',
    'start_date': days_ago(0),
    'email': ['olegtoll@gmail.com'],
    'email_on_failure': True,
    'email_on_retry':True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

# unzip_data = BashOperator(
#     task_id='unzip_data',
#     bash_command='tar zxvf /home/olega/project_airflow/airflow/final_assignment/data/tolldata.tgz',
#     dag=dag,
# )

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = "cut -d':' -f1,2,3,4 /home/olega/project_airflow/airflow/final_assignment/data/vehicle-data.csv > /home/olega/project_airflow/airflow/final_assignment/data/csv_data.csv",
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = "echo \"$(cut -d$'\t' -f4,5 /home/olega/project_airflow/airflow/final_assignment/data/tollplaza-data.tsv)\" | tr '\t' ',' > /home/olega/project_airflow/airflow/final_assignment/data/tsv_data.csv",
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = "echo \"$(cut -c59-61,62-67 /home/olega/project_airflow/airflow/final_assignment/data/payment-data.txt)\" | tr ' ' ',' > /home/olega/project_airflow/airflow/final_assignment/data/fixed_width_data.csv",
    dag=dag,
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command="paste -d ',' /home/olega/project_airflow/airflow/final_assignment/data/csv_data.csv /home/olega/project_airflow/airflow/final_assignment/data/tsv_data.csv /home/olega/project_airflow/airflow/final_assignment/data/fixed_width_data.csv > /home/olega/project_airflow/airflow/final_assignment/data/extracted_data.csv",
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command=r"sed -E 's/^(([^,]+,){3})([^,]+)/\1\\U\\3/' /home/olega/project_airflow/airflow/final_assignment/data/extracted_data.csv  > /home/olega/project_airflow/airflow/final_assignment/transformed_data.csv",
    dag=dag,
)


extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data