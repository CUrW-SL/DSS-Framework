from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import sys

from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, '/home/curw/git/DSS-Framework/local_dss_workflow/plugins/operators')
from dynamic_external_trigger_operator import DynamicTriggerDagRunOperator

sys.path.insert(0, '/home/curw/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/curw/git/DSS-Framework/rule_engine')
from rule_executor import get_next_pump_configurations

prod_dag_name = 'pump_scheduler_v1'
schedule_interval = '*/5 * * * *'
dag_pool = 'pump_scheduler_pool'


def generate_dag_run(context):
    print('***************************init_variable_routine**********************************')
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    run_date = context["execution_date"].to_datetime_string()
    print('init_variable_routine|run_date : ', run_date)
    routines = adapter.get_next_pump_routines(run_date)
    print('init_variable_routine|routines : ', routines)
    next_variable_routines = []
    if routines is not None:
        next_variable_routines = get_next_pump_configurations(adapter, routines)
    else:
        print('No variable routine to schedule.')
    return next_variable_routines


def end_routine():
    print('******rounting has completed**********')


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.strptime('2020-01-12 22:00:00', '%Y-%m-%d %H:%M:%S'),
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

    gen_target_dag_run = DynamicTriggerDagRunOperator(
        task_id='gen_target_dag_run',
        default_trigger='dss_variable_routine_v1',
        python_callable=generate_dag_run,
        pool=dag_pool

    )

    scheduler_end = PythonOperator(
        task_id='scheduler_end',
        python_callable=end_routine,
        trigger_rule='none_failed',
        pool=dag_pool
    )

    scheduler_init >> gen_target_dag_run >> scheduler_end
