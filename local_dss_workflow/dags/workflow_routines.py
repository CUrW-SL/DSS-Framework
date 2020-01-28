from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators import ConditionMultiTriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import sys

sys.path.insert(0, '/home/curw/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/curw/git/DSS-Framework/local_dss_workflow/plugins/operators')
from condition_multi_dag_run_operator import ConditionMultiTriggerDagRunOperator
from dss_unit_completion_sensor import DssUnitSensorOperator

sys.path.insert(0, '/home/curw/git/DSS-Framework/gen_util')
from controller_util import get_triggering_dags, \
    update_workflow_routine_status, \
    set_running_state

prod_dag_name = 'dss_controller_v3'
dag_pool = 'parent_pool'
SKIP = 0


def init_workflow_routine(dag_run, **kwargs):
    print('***************************init_workflow_routine**********************************')
    print('init_workflow_routine|dag_run : ', dag_run)
    routine = dag_run.conf
    print('init_workflow_routine|routine : ', routine)
    if routine is not None:
        db_config = Variable.get('db_config', deserialize_json=True)
        adapter = RuleEngineAdapter.get_instance(db_config)
        routine_id = routine['workflow_routine']['id']
        set_running_state(adapter, routine_id)
        return routine


def dss1_branch_func(**context):
    print('***************************dss1_branch_func**********************************')
    routine = context['task_instance'].xcom_pull(task_ids='init_routine')['workflow_routine']
    print('dss1_branch_func|routine : ', routine)
    routine_id = routine['id']
    print('dss1_branch_func|routine_id : ', routine_id)
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    workflow_routine = adapter.get_workflow_routine_info(routine_id)
    if workflow_routine is not None:
        dss_unit_id = workflow_routine['dss1']
        if dss_unit_id == SKIP:
            return 'dss1_dummy'
        else:
            return 'dss_unit1'
    else:
        return 'dss1_dummy'


# tobe implemented multiple model triggering.
def conditionally_trigger_dss_unit1(context):
    print('***************************conditionally_trigger_dss_unit1**********************************')
    print('conditionally_trigger_dss_unit1')
    """This function decides whether or not to Trigger the remote DAG"""
    routine = context['task_instance'].xcom_pull(task_ids='init_routine')['workflow_routine']
    print('conditionally_trigger_dss_unit1|routine : ', routine)
    routine_id = routine['id']
    print('dss1_branch_func|routine_id : ', routine_id)
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    workflow_routine = adapter.get_workflow_routine_info(routine_id)
    if workflow_routine is not None:
        dss_unit_id = workflow_routine['dss1']
        print('dss1_branch_func|dss_unit_id : ', dss_unit_id)
        if context['params']['check_rules']:
            dag_info = get_triggering_dags(adapter, dss_unit_id, 'wrf')
            if len(dag_info):
                return dag_info
            else:
                return []
        else:
            return []
    else:
        return []


def dss2_branch_func(**context):
    print('***************************dss2_branch_func**********************************')
    routine = context['task_instance'].xcom_pull(task_ids='init_routine')['workflow_routine']
    print('dss1_branch_func|routine : ', routine)
    routine_id = routine['id']
    print('dss2_branch_func|routine_id : ', routine_id)
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    workflow_routine = adapter.get_workflow_routine_info(routine_id)
    if workflow_routine is not None:
        dss_unit_id = workflow_routine['dss2']
        if dss_unit_id == SKIP:
            return 'dss2_dummy'
        else:
            return 'dss_unit2'
    else:
        return 'dss2_dummy'


def conditionally_trigger_dss_unit2(context):
    print('***************************conditionally_trigger_dss_unit2**********************************')
    print('conditionally_trigger_dss_unit2')
    """This function decides whether or not to Trigger the remote DAG"""
    routine = context['task_instance'].xcom_pull(task_ids='init_routine')['workflow_routine']
    print('dss1_branch_func|routine : ', routine)
    routine_id = routine['id']
    print('dss1_branch_func|routine_id : ', routine_id)
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    workflow_routine = adapter.get_workflow_routine_info(routine_id)
    if workflow_routine is not None:
        dss_unit_id = workflow_routine['dss2']
        print('dss2_branch_func|dss_unit_id : ', dss_unit_id)
        if context['params']['check_rules']:
            dag_info = get_triggering_dags(adapter, dss_unit_id, 'hechms')
            if len(dag_info):
                return dag_info
            else:
                return []
        else:
            return []
    else:
        return []


def dss3_branch_func(**context):
    print('***************************dss3_branch_func**********************************')
    routine = context['task_instance'].xcom_pull(task_ids='init_routine')['workflow_routine']
    print('dss1_branch_func|routine : ', routine)
    routine_id = routine['id']
    print('dss2_branch_func|routine_id : ', routine_id)
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    workflow_routine = adapter.get_workflow_routine_info(routine_id)
    if workflow_routine is not None:
        dss_unit_id = workflow_routine['dss3']
        if dss_unit_id == SKIP:
            return 'dss3_dummy'
        else:
            return 'dss_unit3'
    else:
        return 'dss3_dummy'


def conditionally_trigger_dss_unit3(context):
    print('***************************conditionally_trigger_dss_unit3**********************************')
    print('conditionally_trigger_dss_unit3')
    """This function decides whether or not to Trigger the remote DAG"""
    routine = context['task_instance'].xcom_pull(task_ids='init_routine')['workflow_routine']
    print('dss1_branch_func|routine : ', routine)
    routine_id = routine['id']
    print('dss1_branch_func|routine_id : ', routine_id)
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    workflow_routine = adapter.get_workflow_routine_info(routine_id)
    if workflow_routine is not None:
        dss_unit_id = workflow_routine['dss3']
        print('dss3_branch_func|dss_unit_id : ', dss_unit_id)
        if context['params']['check_rules']:
            dag_info = get_triggering_dags(adapter, dss_unit_id, 'flo2d')
            if len(dag_info):
                return dag_info
            else:
                return []
        else:
            return []
    else:
        return []


def end_workflow_routine(**context):
    print('***************************end_workflow_routine**********************************')
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
         catchup=False, description='Run DSS Controller DAG') as dag:
    init_routine = PythonOperator(
        task_id='init_routine',
        python_callable=init_workflow_routine,
        provide_context=True,
        pool=dag_pool
    )

    dss1_branch = BranchPythonOperator(
        task_id='dss1_branch',
        provide_context=True,
        python_callable=dss1_branch_func,
        pool=dag_pool
    )

    dss1_dummy = DummyOperator(
        task_id='dss1_dummy',
        pool=dag_pool
    )

    dss_unit1 = ConditionMultiTriggerDagRunOperator(
        task_id='dss_unit1',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit1,
        params={'check_rules': True, 'rule_types': ['wrf']},
        pool=dag_pool
    )

    check_dss1_completion = DssUnitSensorOperator(
        task_id='check_dss1_completion',
        poke_interval=60,
        timeout=3600,
        params={'model': 'wrf', 'dss': 'dss1'},
        provide_context=True,
        trigger_rule='none_failed'
    )

    dss2_branch = BranchPythonOperator(
        task_id='dss2_branch',
        provide_context=True,
        python_callable=dss2_branch_func,
        trigger_rule='none_failed',
        pool=dag_pool
    )

    dss2_dummy = DummyOperator(
        task_id='dss2_dummy',
        pool=dag_pool
    )

    dss_unit2 = ConditionMultiTriggerDagRunOperator(
        task_id='dss_unit2',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit2,
        params={'check_rules': True, 'rule_types': ['hechms']},
        pool=dag_pool
    )

    check_dss2_completion = DssUnitSensorOperator(
        task_id='check_dss2_completion',
        poke_interval=60,
        timeout=3600,
        params={'model': 'hechms', 'dss': 'dss2'},
        provide_context=True,
        trigger_rule='none_failed',
    )

    dss3_branch = BranchPythonOperator(
        task_id='dss3_branch',
        provide_context=True,
        python_callable=dss3_branch_func,
        trigger_rule='none_failed',
        pool=dag_pool
    )

    dss3_dummy = DummyOperator(
        task_id='dss3_dummy',
        pool=dag_pool
    )

    dss_unit3 = ConditionMultiTriggerDagRunOperator(
        task_id='dss_unit3',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit3,
        params={'check_rules': True, 'rule_types': ['flo2d']},
        pool=dag_pool
    )

    check_dss3_completion = DssUnitSensorOperator(
        task_id='check_dss3_completion',
        poke_interval=60,
        timeout=3600,
        params={'model': 'flo2d', 'dss': 'dss3'},
        provide_context=True,
        trigger_rule='none_failed'
    )

    end_routine = PythonOperator(
        task_id='end_routine',
        python_callable=end_workflow_routine,
        trigger_rule='none_failed',
        provide_context=True,
        pool=dag_pool
    )

    init_routine >> \
    dss1_branch >> [dss_unit1, dss1_dummy] >> \
    check_dss1_completion >> dss2_branch >> [dss_unit2, dss2_dummy] >> \
    check_dss2_completion >> dss3_branch >> [dss_unit3, dss3_dummy] >> \
    check_dss3_completion >> end_routine
