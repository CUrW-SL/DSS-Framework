from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


prod_dag_name = 'test_dag'
schedule_interval = '*/5 * * * *'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.strptime('2019-11-01 12:30:00', '%Y-%m-%d %H:%M:%S'),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

test_task1_cmd = 'echo "download_gfs_cmd" ;sleep $[($RANDOM % 10) + 1]s'
test_task2_cmd = 'echo "run_wrf_A_cmd" ;sleep $[($RANDOM % 1000) + 1]s'
test_task3_cmd = 'echo "rfield_gen_cmd" ;sleep $[($RANDOM % 100) + 1]s'


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run TEST DAG') as dag:
    test_task1 = BashOperator(
        task_id='test_task1',
        bash_command=test_task1_cmd,
    )

    test_task2 = BashOperator(
        task_id='test_task2',
        bash_command=test_task2_cmd,
    )

    test_task3 = BashOperator(
        task_id='test_task3',
        bash_command=test_task3_cmd,
    )

    test_task1 >> test_task2 >> test_task3

