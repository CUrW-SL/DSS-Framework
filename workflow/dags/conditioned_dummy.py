from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import random


prod_dag_name = 'conditioned_dummy'
schedule_interval = '*/10 * * * *'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.strptime('2019-09-23 07:00:00', '%Y-%m-%d %H:%M:%S'),
    'email': ['hasithadkr7.com'],
    'email_on_failure': True,
}


def branch1_func(**context):
    return random.choice(['dss1_dummy', 'dss2_dummy', 'dss3_dummy'])


def branch2_func(**context):
    return random.choice(['dss5_dummy', 'dss6_dummy'])


def branch3_func(**context):
    return random.choice(['dss7_dummy', 'dss8_dummy'])


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=schedule_interval,
         description='Dummy operator test DAG') as dag:
    init = DummyOperator(
        task_id='init'
    )

    branch1 = BranchPythonOperator(
        task_id='branch1',
        provide_context=True,
        python_callable=branch1_func
    )

    dss1_dummy = DummyOperator(
        task_id='dss1_dummy'
    )

    dss2_dummy = DummyOperator(
        task_id='dss2_dummy'
    )

    dss3_dummy = DummyOperator(
        task_id='dss3_dummy'
    )

    dss4_dummy = DummyOperator(
        task_id='dss4_dummy'
    )

    branch2 = BranchPythonOperator(
        task_id='branch2',
        provide_context=True,
        python_callable=branch2_func
    )

    dss5_dummy = DummyOperator(
        task_id='dss5_dummy'
    )

    dss6_dummy = DummyOperator(
        task_id='dss6_dummy'
    )

    branch3 = BranchPythonOperator(
        task_id='branch3',
        provide_context=True,
        python_callable=branch3_func
    )

    dss7_dummy = DummyOperator(
        task_id='dss7_dummy'
    )

    dss8_dummy = DummyOperator(
        task_id='dss8_dummy'
    )

    end_routine = DummyOperator(
        task_id='end_routine'
    )

    init >> \
    branch1 >> [dss1_dummy, dss2_dummy, dss3_dummy] >> \
    dss4_dummy >> \
    branch2 >> [dss5_dummy, dss6_dummy] >> \
    branch3 >> [dss7_dummy, dss8_dummy] >> \
    end_routine

