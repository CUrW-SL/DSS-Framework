from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import sys

print(sys.path)
sys.path.insert(0, '/home/hasitha/PycharmProjects/DSS-Framework/database')

prod_dag_name = 'dummy_workflow'
schedule_interval = '*/10 * * * *'


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.strptime('2019-09-23 06:30:00', '%Y-%m-%d %H:%M:%S'),
    'email': ['hasithadkr7.com'],
    'email_on_failure': True,
}

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=schedule_interval,
         description='Dummy operator test DAG') as dag:

    dss1_dummy = DummyOperator(
        task_id='dss1_dummy'
    )

    dss2_dummy = DummyOperator(
        task_id='dss2_dummy'
    )

    dss3_dummy = DummyOperator(
        task_id='dss3_dummy'
    )

    end_routine = DummyOperator(
        task_id='end_routine'
    )

    dss1_dummy >> dss2_dummy >> dss3_dummy >> end_routine
