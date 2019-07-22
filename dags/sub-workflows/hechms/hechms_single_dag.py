from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


prod_dag_name = 'hec-hms-single-dag'
queue = 'default'
schedule_interval = '10 * * * *'
dag_pool = 'curw_prod_runs'


default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0, hour=6),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'queue': queue,
    'catchup': False,
}

# initiate the DAG
dag = DAG(
    prod_dag_name,
    default_args=default_args,
    description='Run HecHms DAG',
    schedule_interval=schedule_interval)


create_rainfall_cmd = 'echo "create_rainfall_cmd"'

run_hechms_single_cmd = 'echo "run_hechms_single_cmd"'

upload_discharge_cmd = 'echo "upload_discharge_cmd"'

create_rainfall = BashOperator(
    task_id='create_rainfall',
    bash_command=create_rainfall_cmd,
    dag=dag,
    pool=dag_pool,
)

run_hechms_single = BashOperator(
    task_id='run_hechms_single',
    bash_command=run_hechms_single_cmd,
    dag=dag,
    pool=dag_pool,
)

upload_discharge = BashOperator(
    task_id='upload_discharge',
    bash_command=upload_discharge_cmd,
    dag=dag,
    pool=dag_pool,
)

create_rainfall >> run_hechms_single >> upload_discharge

