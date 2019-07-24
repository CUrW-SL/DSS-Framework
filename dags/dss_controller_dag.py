from datetime import datetime

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator

prod_dag_name = 'dss_controller_dag'
schedule_interval = '*/5 * * * *'


def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return dag_run_obj


default_args = {
        "owner": "dss admin",
        "start_date": datetime.strptime('2019-07-24 02:30:00', '%Y-%m-%d %H:%M:%S'),
    }

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run DSS Controller DAG') as dag:
    # Define the single task in this controller example DAG
    dss_unit1 = TriggerDagRunOperator(
        task_id='dss_unit1',
        trigger_dag_id="wrfv4-pre-dag",
        python_callable=conditionally_trigger,
        params={'condition_param': True, 'message': 'DSS UNIT 1'},
        dag=dag,
    )

    dss_unit2 = TriggerDagRunOperator(
        task_id='dss_unit2',
        trigger_dag_id="dss_trigger_target_dag",
        python_callable=conditionally_trigger,
        params={'condition_param': True, 'message': 'DSS UNIT 2'},
        dag=dag,
    )

    dss_unit3 = TriggerDagRunOperator(
        task_id='dss_unit3',
        trigger_dag_id="dss_trigger_target_dag",
        python_callable=conditionally_trigger,
        params={'condition_param': True, 'message': 'DSS UNIT 3'},
        dag=dag,
    )

    dss_unit4 = TriggerDagRunOperator(
        task_id='dss_unit4',
        trigger_dag_id="dss_trigger_target_dag",
        python_callable=conditionally_trigger,
        params={'condition_param': True, 'message': 'DSS UNIT 4'},
        dag=dag,
    )

    dss_unit1 >> dss_unit2 >> dss_unit3 >> dss_unit4

