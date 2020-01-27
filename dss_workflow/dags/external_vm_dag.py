from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

prod_dag_name = 'external_vm_dag1'
schedule_interval = '*/5 * * * *'
dag_pool = 'test_pool'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.strptime('2020-01-27 08:40:00', '%Y-%m-%d %H:%M:%S'),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

bash_command = ''

with DAG(dag_id=prod_dag_name, default_args=default_args,
         catchup=False, schedule_interval=schedule_interval,
         description='Run DSS Controller DAG') as dag:
    init = DummyOperator(
        task_id='init',
        pool=dag_pool
    )

    run_bash = BashOperator(
        task_id='run_bash',
        bash_command=bash_command,
        execution_timeout=timedelta(minutes=1),
        dag=dag,
        pool=dag_pool,
    )

    end = DummyOperator(
        task_id='end',
        pool=dag_pool
    )

    init >> run_bash >> end
