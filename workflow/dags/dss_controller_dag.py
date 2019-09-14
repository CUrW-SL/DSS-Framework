from datetime import datetime
from airflow import DAG
from airflow.operators import ConditionTriggerDagRunOperator

prod_dag_name = 'dss_controller_dag'
schedule_interval = '*/10 * * * *'


def conditionally_trigger_dss_unit1(context, dag_run_obj):
    print('conditionally_trigger_dss_unit1')
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['check_rules']:
        allowed_rule_types = context['params']['rule_types']
        dag_run_obj.payload = {'message': context['params']['message']}
        return {'trigger_dag_id': 'wrfv4-pre-dag', 'dro': dag_run_obj}


def conditionally_trigger_dss_unit2(context, dag_run_obj):
    print('conditionally_trigger_dss_unit2')
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['check_rules']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return {'trigger_dag_id': 'wrfv4-E-dag', 'dro': dag_run_obj}


def conditionally_trigger_dss_unit3(context, dag_run_obj):
    print('conditionally_trigger_dss_unit3')
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['check_rules']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return {'trigger_dag_id': 'hec-hms-single-dag', 'dro': dag_run_obj}


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
        params={'check_rules': True, 'rule_types': ['wrf']},
        dag=dag,
    )

    dss_unit2 = ConditionTriggerDagRunOperator(
        task_id='dss_unit2',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit2,
        params={'check_rules': True, 'rule_types': ['hechms']},
        dag=dag,
    )

    dss_unit3 = ConditionTriggerDagRunOperator(
        task_id='dss_unit3',
        default_trigger="dss_trigger_target_dag",
        python_callable=conditionally_trigger_dss_unit3,
        params={'check_rules': True, 'rule_types': ['flo2d']},
        dag=dag,
    )

    dss_unit1 >> dss_unit2 >> dss_unit3

