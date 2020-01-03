from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import DagRunOrder
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import sys

from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/dss_workflow/plugins/operators')
from multi_dag_trigger_operator import TriggerMultiDagRunOperator

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

prod_dag_name = 'variable_scheduler_v1'
schedule_interval = '*/5 * * * *'
dag_pool = 'variable_scheduler_pool'


def generate_dag_run(context):
    print('***************************init_variable_routine**********************************')
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    run_date = context["execution_date"].to_datetime_string()
    print('init_variable_routine|run_date : ', run_date)
    routines = adapter.get_next_variable_routines(run_date)
    print('init_variable_routine|routines : ', routines)
    next_variable_routines = []
    if routines is not None:
        for variable_routine in routines:
            next_variable_routines.append(DagRunOrder(payload={'variable_routine': variable_routine}))
    else:
        print('No variable routine to schedule.')
    return next_variable_routines


def end_routine():
    print('******rounting has completed**********')


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.strptime('2020-01-04 04:00:00', '%Y-%m-%d %H:%M:%S'),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(dag_id=prod_dag_name, default_args=default_args,
         catchup=False, schedule_interval=schedule_interval,
         description='Run DSS Controller DAG') as dag:
    scheduler_init = DummyOperator(
        task_id='scheduler_init',
        pool=dag_pool
    )

    gen_target_dag_run = TriggerMultiDagRunOperator(
        task_id='gen_target_dag_run',
        trigger_dag_id='dss_variable_routine_v1',
        python_callable=generate_dag_run,
        provide_context=True,
        pool=dag_pool
    )

    scheduler_end = PythonOperator(
        task_id='scheduler_end',
        python_callable=end_routine,
        trigger_rule='none_failed',
        provide_context=True,
        pool=dag_pool
    )

    scheduler_init >> gen_target_dag_run >> scheduler_end
