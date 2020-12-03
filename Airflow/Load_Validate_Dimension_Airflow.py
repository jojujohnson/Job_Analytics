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
    'start_date': datetime(2020, 9, 4, 4, 0, 0),
    'email': ['airflow@example.com'],
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
    'Load_Validate_Dimension',
    default_args=default_args,
    description='DAG to perform Dimension loading and verification',
    schedule_interval='0 */8 * * *',

)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='load_dimension_exclude_keyword',
    depends_on_past=False,
    bash_command='python3 /home/ubuntu/JobAnalytics/Scripts/db_operations.py load_dimension_exclude_keyword ',
    retries=1,
    dag=dag,
)

t2 = BashOperator(
    task_id='load_location_job_dimension',
    depends_on_past=False,
    bash_command='python3 /home/ubuntu/JobAnalytics/Scripts/db_operations.py load_location_job_dimension ',
    retries=1,
    dag=dag,
)

t3 = BashOperator(
    task_id='validate_location_dimension',
    depends_on_past=False,
    bash_command='python3 /home/ubuntu/JobAnalytics/Scripts/Validator.py validate_location_dimension ',
    retries=1,
    dag=dag,
)

t4 = BashOperator(
    task_id='validate_job_dimension',
    depends_on_past=False,
    bash_command='python3 /home/ubuntu/JobAnalytics/Scripts/Validator.py validate_job_dimension ',
    retries=1,
    dag=dag,
)

t5 = BashOperator(
    task_id='select_db_details',
    depends_on_past=False,
    bash_command='python3 /home/ubuntu/JobAnalytics/Scripts/db_operations.py db_details ',
    retries=1,
    dag=dag,
)

t6 = BashOperator(
    task_id='load_fact',
    depends_on_past=False,
    bash_command='python3 /home/ubuntu/JobAnalytics/Scripts/db_operations.py load_fact ',
    retries=1,
    dag=dag,
)

t7 = BashOperator(
    task_id='load_summary',
    depends_on_past=False,
    bash_command='python3 /home/ubuntu/JobAnalytics/Scripts/db_operations.py load_summary ',
    retries=1,
    dag=dag,
)

t1 >> t2 >> t6 >> t3 >> t7 >> t5
t2 >> t6 >> t4 >> t7 >> t5

