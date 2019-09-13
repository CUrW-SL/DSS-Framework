import pprint
from datetime import datetime

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator

pp = pprint.PrettyPrinter(indent=4)


def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        pp.pprint(dag_run_obj.payload)
        return dag_run_obj


# Define the DAG
dag = DAG(
    dag_id='dss_controller_dag',   default_args={
        "owner": "dss admin",
        "start_date": datetime.strptime('2019-07-23 09:30:00', '%Y-%m-%d %H:%M:%S'),
    },
    schedule_interval='*/5 * * * *',  # should have schedule interval for executing tasks.
)

# Define the single task in this controller example DAG
trigger = TriggerDagRunOperator(
    task_id='dss_trigger_dagrun',
    trigger_dag_id="dss_trigger_target_dag",
    python_callable=conditionally_trigger,
    params={'condition_param': True, 'message': 'Hello Hasitha'},
    dag=dag,
)

