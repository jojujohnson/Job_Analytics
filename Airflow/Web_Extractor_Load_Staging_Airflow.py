# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 3),
    'email': ['joju.johnson@outlook.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'Web_Extractor_Load_Staging',
    default_args=default_args,
    description='DAG to perform Load to Company Dimension , Crawl Web for Job Data and Load to Web Extractor staging',
    schedule_interval='30 4 * * SUN-FRI',
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='load_company_dimension',
    bash_command='python3 /home/ubuntu/JobAnalytics/Scripts/db_operations.py load_company_dimension ',
    dag=dag,
)

t2 = BashOperator(
    task_id='crawl_web_and_extract',
    bash_command='python3 /home/ubuntu/JobAnalytics/Scripts/Web_Extractor.py ',
    dag=dag,
)

t3 = BashOperator(
    task_id='load_web_extractor_staging',
    depends_on_past=False,
    bash_command='python3 /home/ubuntu/JobAnalytics/Scripts/db_operations.py load_web_extractor_staging ',
    retries=1,
    dag=dag,
)

t1 >> t2 >> t3
