from datetime import datetime
from airflow import DAG
from airflow.operators import ConditionMultiTriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import sys
sys.path.insert(0, '/home/hasitha/PycharmProjects/DSS-Framework/db_util')
from db_adapter import RuleEngineAdapter
sys.path.insert(0, '/home/hasitha/PycharmProjects/DSS-Framework/gen_util')
from controller_util import get_triggering_dags


prod_dag_name = 'dss_controller_dag3'
schedule_interval = '*/5 * * * *'
SKIP = 0


def init_workflow_routine(**context):
    print('***************************init_workflow_routine**********************************')
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    run_date = context["execution_date"].to_datetime_string()
    print('init_workflow_routine|run_date : ', run_date)
    routine = adapter.get_next_workflow_routines(run_date)
    print('init_workflow_routine|routine : ', routine)
    if routine is None:
        return {'id': 0, 'dss1': 0, 'dss2': 0, 'dss3': 0}
    else:
        return routine


def dss1_branch_func(**context):
    print('***************************dss1_branch_func**********************************')
    dss1_rule = context['task_instance'].xcom_pull(task_ids='init_routine')['dss1']
    print('dss1_branch_func|dss1_rule : ', dss1_rule)
    if dss1_rule == SKIP:
        return 'dss1_dummy'
    else:
        return 'dss_unit1'


# tobe implemented multiple model triggering.
def conditionally_trigger_dss_unit1(context, dag_run_obj):
    print('***************************conditionally_trigger_dss_unit1**********************************')
    print('conditionally_trigger_dss_unit1')
    """This function decides whether or not to Trigger the remote DAG"""
    dss1_rule_id = context['task_instance'].xcom_pull(task_ids='init_routine')['dss1']
    print('dss1_branch_func|dss1_rule : ', dss1_rule_id)
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    if context['params']['check_rules']:
        dag_info = get_triggering_dags(adapter, dss1_rule_id, 'wrf')
        if len(dag_info):
            return dag_info
        else:
            return []


def dss2_branch_func(**context):
    print('***************************dss2_branch_func**********************************')
    dss2_rule = context['task_instance'].xcom_pull(task_ids='init_routine')['dss2']
    print('dss2_rule : ', dss2_rule)
    if dss2_rule == SKIP:
        return 'dss2_dummy'
    else:
        return 'dss_unit2'


def conditionally_trigger_dss_unit2(context, dag_run_obj):
    print('***************************conditionally_trigger_dss_unit2**********************************')
    print('conditionally_trigger_dss_unit2')
    """This function decides whether or not to Trigger the remote DAG"""
    dss2_rule_id = context['task_instance'].xcom_pull(task_ids='init_routine')['dss2']
    print('dss2_branch_func|dss2_rule_id : ', dss2_rule_id)
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    if context['params']['check_rules']:
        dag_info = get_triggering_dags(adapter, dss2_rule_id, 'hechms')
        if len(dag_info):
            return dag_info
        else:
            return []


def dss3_branch_func(**context):
    print('***************************dss3_branch_func**********************************')
    dss3_rule = context['task_instance'].xcom_pull(task_ids='init_routine')['dss3']
    print('dss3_rule : ', dss3_rule)
    if dss3_rule == SKIP:
        return 'dss3_dummy'
    else:
        return 'dss_unit3'


def conditionally_trigger_dss_unit3(context, dag_run_obj):
    print('***************************conditionally_trigger_dss_unit3**********************************')
    print('conditionally_trigger_dss_unit3')
    """This function decides whether or not to Trigger the remote DAG"""
    dss2_rule_id = context['task_instance'].xcom_pull(task_ids='init_routine')['dss3']
    print('dss3_branch_func|dss2_rule_id : ', dss2_rule_id)
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    if context['params']['check_rules']:
        dag_info = get_triggering_dags(adapter, dss2_rule_id, 'flo2d')
        if len(dag_info):
            return dag_info
        else:
            return []

default_args = {
        'owner': 'dss admin',
        'start_date': datetime.strptime('2019-10-09 17:00:00', '%Y-%m-%d %H:%M:%S'),
        'email': ['hasithadkr7.com'],
        'email_on_failure': True,
    }

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=schedule_interval,
         description='Run DSS Controller DAG') as dag:

    init_routine = PythonOperator(
        task_id='init_routine',
        python_callable=init_workflow_routine,
        provide_context=True
    )

    dss1_branch = BranchPythonOperator(
        task_id='dss1_branch',
        provide_context=True,
        python_callable=dss1_branch_func
    )

    dss1_dummy = DummyOperator(
        task_id='dss1_dummy'
    )

    dss_unit1 = ConditionMultiTriggerDagRunOperator(
        task_id='dss_unit1',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit1,
        params={'check_rules': True, 'rule_types': ['wrf']}
    )

    dss2_branch = BranchPythonOperator(
        task_id='dss2_branch',
        provide_context=True,
        python_callable=dss2_branch_func,
        trigger_rule='none_failed'
    )

    dss2_dummy = DummyOperator(
        task_id='dss2_dummy'
    )

    dss_unit2 = ConditionMultiTriggerDagRunOperator(
        task_id='dss_unit2',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit2,
        params={'check_rules': True, 'rule_types': ['hechms']}
    )

    dss3_branch = BranchPythonOperator(
        task_id='dss3_branch',
        provide_context=True,
        python_callable=dss3_branch_func,
        trigger_rule='none_failed'
    )

    dss3_dummy = DummyOperator(
        task_id='dss3_dummy'
    )

    dss_unit3 = ConditionMultiTriggerDagRunOperator(
        task_id='dss_unit3',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit3,
        params={'check_rules': True, 'rule_types': ['flo2d']}
    )

    end_routine = DummyOperator(
        task_id='end_routine',
        trigger_rule='none_failed'
    )

    init_routine >> \
    dss1_branch >> [dss_unit1, dss1_dummy] >> \
    dss2_branch >> [dss_unit2, dss2_dummy] >> \
    dss3_branch >> [dss_unit3, dss3_dummy] >> \
    end_routine

