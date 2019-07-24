from datetime import datetime

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
# from airflow.operators import ConditionTriggerDagRunOperator
from condition_dag_run_opearator import ConditionTriggerDagRunOperator
#from airflow.operators.condition_dag_run_opearator import ConditionTriggerDagRunOperator

prod_dag_name = 'dss_controller_dag'
schedule_interval = '*/10 * * * *'


def conditionally_trigger_dss_unit1(context, dag_run_obj):
    print('conditionally_trigger_dss_unit1')
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return {'trigger_dag_id': 'wrfv4-pre-dag', 'dro': dag_run_obj}


def conditionally_trigger_dss_unit2(context, dag_run_obj):
    print('conditionally_trigger_dss_unit2')
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return {'trigger_dag_id': 'wrfv4-E-dag', 'dro': dag_run_obj}


def conditionally_trigger_dss_unit3(context, dag_run_obj):
    print('conditionally_trigger_dss_unit3')
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return {'trigger_dag_id': 'hec-hms-single-dag', 'dro': dag_run_obj}


def conditionally_trigger_dss_unit4(context, dag_run_obj):
    print('conditionally_trigger_dss_unit4')
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return {'trigger_dag_id': 'flo2d-250m-dag', 'dro': dag_run_obj}


default_args = {
        'owner': 'dss admin',
        'start_date': datetime.strptime('2019-07-24 11:00:00', '%Y-%m-%d %H:%M:%S'),
        'email': ['hasithadkr7.com'],
        'email_on_failure': True,
    }

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run DSS Controller DAG') as dag:

    dss_unit1 = ConditionTriggerDagRunOperator(
        task_id='dss_unit1',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit1,
        params={'condition_param': True, 'message': 'DSS UNIT 1'},
        dag=dag,
    )

    dss_unit2 = ConditionTriggerDagRunOperator(
        task_id='dss_unit2',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit2,
        params={'condition_param': True, 'message': 'DSS UNIT 2'},
        dag=dag,
    )

    dss_unit3 = ConditionTriggerDagRunOperator(
        task_id='dss_unit3',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit3,
        params={'condition_param': True, 'message': 'DSS UNIT 3'},
        dag=dag,
    )

    dss_unit4 = ConditionTriggerDagRunOperator(
        task_id='dss_unit4',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit4,
        params={'condition_param': True, 'message': 'DSS UNIT 4'},
        dag=dag,
    )

    dss_unit1 >> dss_unit2 >> dss_unit3 >> dss_unit4

