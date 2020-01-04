from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

prod_dag_name = 'water_level_rising_rate_dag'
dag_pool = 'water_level_rate_pool'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run water_level_rising_rate_dag DAG', catchup=False) as dag:
    task1 = DummyOperator(
        task_id='task1',
        pool=dag_pool
    )

    task2 = DummyOperator(
        task_id='task2',
        pool=dag_pool
    )

    task3 = DummyOperator(
        task_id='task3',
        pool=dag_pool
    )

    task4 = DummyOperator(
        task_id='task4',
        pool=dag_pool
    )

    task5 = DummyOperator(
        task_id='task5',
        pool=dag_pool
    )

    task1 >> task2 >> task3 >> task4 >> task5
