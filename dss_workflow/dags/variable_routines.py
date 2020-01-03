from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators import ConditionMultiTriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import sys

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/dss_workflow/plugins/operators')
from condition_multi_dag_run_operator import ConditionMultiTriggerDagRunOperator
from dss_unit_completion_sensor import DssUnitSensorOperator

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/gen_util')
from controller_util import get_triggering_dags, \
    update_workflow_routine_status, \
    set_running_state

prod_dag_name = 'variable_controller_v1'
dag_pool = 'variable_controller_pool'
SKIP = 0


def init_variable_routine(dag_run, **kwargs):
    print('***************************init_variable_routine**********************************')
    print('init_variable_routine|dag_run : ', dag_run)
    routine = dag_run.conf
    print('init_variable_routine|routine : ', routine)
    if routine is not None:
        db_config = Variable.get('db_config', deserialize_json=True)
        adapter = RuleEngineAdapter.get_instance(db_config)
        routine_id = routine['workflow_routine']['id']
        set_running_state(adapter, routine_id)
        return routine


def end_variable_routine(**context):
    print('***************************end_variable_routine**********************************')
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    update_workflow_routine_status(adapter)
    print('******rounting completed**********')


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.strptime('2019-11-03 02:15:00', '%Y-%m-%d %H:%M:%S'),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         catchup=False, description='Run Variable Controller DAG') as dag:
    init_routine = PythonOperator(
        task_id='init_routine',
        python_callable=init_variable_routine,
        provide_context=True,
        pool=dag_pool
    )

    end_routine = PythonOperator(
        task_id='end_routine',
        python_callable=end_variable_routine,
        trigger_rule='none_failed',
        provide_context=True,
        pool=dag_pool
    )

    init_routine >> end_routine
