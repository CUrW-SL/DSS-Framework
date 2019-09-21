from datetime import datetime
from airflow import DAG
from airflow.operators import ConditionTriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import sys
print(sys.path)
sys.path.insert(0, '/home/hasitha/PycharmProjects/DSS-Framework/database')
from db_adapter import RuleEngineAdapter


prod_dag_name = 'dss_controller_dag1'
schedule_interval = '*/10 * * * *'
SKIP = 0


def init_workflow_routine(**context):
    db_config = Variable.get('db_config', deserialize_json=True)
    print('init_workflow_routine|db_config : ', db_config)
    adapter = RuleEngineAdapter.get_instance(db_config)

    run_date = datetime.strptime(context["execution_date"], '%Y-%m-%d %H:%M:%S')
    print('init_workflow_routine|run_date : ', run_date)
    routine = adapter.get_next_workflow_routine(run_date)
    task_instance = context['task_instance']
    task_instance.xcom_push('routine', routine)


def dss1_branch_func(**kwargs):
    ti = kwargs['ti']
    routine = int(ti.xcom_pull(key='routine', task_ids='init_routine'))
    dss1_rule = routine['dss1']
    print('dss1_rule : ', dss1_rule)
    if dss1_rule == SKIP:
        return 'dss1_dummy'
    else:
        return 'dss_unit1'


def conditionally_trigger_dss_unit1(context, dag_run_obj):
    print('conditionally_trigger_dss_unit1')
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['check_rules']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['check_rules']:
        allowed_rule_types = context['params']['rule_types']
        dag_run_obj.payload = {'message': context['params']['rule_types']}
        return {'trigger_dag_id': 'wrf_4_E_dag', 'dro': dag_run_obj}


def dss2_branch_func(**kwargs):
    ti = kwargs['ti']
    routine = int(ti.xcom_pull(key='routine', task_ids='init_routine'))
    dss2_rule = routine['dss2']
    print('dss2_rule : ', dss2_rule)
    if dss2_rule == SKIP:
        return 'dss2_dummy'
    else:
        return 'dss_unit2'


def conditionally_trigger_dss_unit2(context, dag_run_obj):
    print('conditionally_trigger_dss_unit2')
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['check_rules']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['check_rules']:
        dag_run_obj.payload = {'message': context['params']['rule_types']}
        return {'trigger_dag_id': 'hechms_single_dag', 'dro': dag_run_obj}


def dss3_branch_func(**kwargs):
    ti = kwargs['ti']
    routine = int(ti.xcom_pull(key='routine', task_ids='init_routine'))
    dss3_rule = routine['dss3']
    print('dss3_rule : ', dss3_rule)
    if dss3_rule == SKIP:
        return 'dss3_dummy'
    else:
        return 'dss_unit3'


def conditionally_trigger_dss_unit3(context, dag_run_obj):
    print('conditionally_trigger_dss_unit3')
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['check_rules']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['check_rules']:
        dag_run_obj.payload = {'message': context['params']['rule_types']}
        return {'trigger_dag_id': 'flo2d_250m_dag', 'dro': dag_run_obj}


default_args = {
        'owner': 'dss admin',
        'start_date': datetime.strptime('2019-09-21 16:00:00', '%Y-%m-%d %H:%M:%S'),
        'email': ['hasithadkr7.com'],
        'email_on_failure': True,
    }

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=schedule_interval,
         description='Run DSS Controller DAG') as dag:

    init_routine = PythonOperator(
        task_id='init_routine',
        python_callable=init_workflow_routine,
        provide_context=True,
        dag=dag,
    )

    dss1_branch = BranchPythonOperator(
        task_id='dss1_branch',
        provide_context=True,
        python_callable=dss1_branch_func
    )

    dss1_dummy = DummyOperator(
        task_id='dss1_dummy'
    )

    dss_unit1 = ConditionTriggerDagRunOperator(
        task_id='dss_unit1',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit1,
        params={'check_rules': True, 'rule_types': ['wrf']}
    )

    dss2_branch = BranchPythonOperator(
        task_id='dss2_branch',
        provide_context=True,
        python_callable=dss2_branch_func
    )

    dss2_dummy = DummyOperator(
        task_id='dss2_dummy'
    )

    dss_unit2 = ConditionTriggerDagRunOperator(
        task_id='dss_unit2',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit2,
        params={'check_rules': True, 'rule_types': ['hechms']}
    )

    dss3_branch = BranchPythonOperator(
        task_id='dss3_branch',
        provide_context=True,
        python_callable=dss3_branch_func
    )

    dss3_dummy = DummyOperator(
        task_id='dss3_dummy'
    )

    dss_unit3 = ConditionTriggerDagRunOperator(
        task_id='dss_unit3',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit3,
        params={'check_rules': True, 'rule_types': ['flo2d']}
    )

    end_routine = DummyOperator(
        task_id='end_routine'
    )

    init_routine >> \
    dss1_branch >> [dss_unit1, dss1_dummy] >> \
    dss2_branch >> [dss_unit2, dss2_dummy] >> \
    dss3_branch >> [dss_unit3, dss3_dummy] >> \
    end_routine

