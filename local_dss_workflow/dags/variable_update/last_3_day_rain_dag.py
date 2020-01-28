from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import sys

sys.path.insert(0, '/home/curw/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

prod_dag_name = 'last_3_day_rain_dag'
dag_pool = 'last_3_day_rain_pool'


def update_workflow_status(status, rule_id):
    try:
        db_config = Variable.get('db_config', deserialize_json=True)
        try:
            adapter = RuleEngineAdapter.get_instance(db_config)
            adapter.update_variable_routing_status(status, rule_id)
        except Exception as ex:
            print('update_workflow_status|db_adapter|Exception: ', str(ex))
    except Exception as e:
        print('update_workflow_status|db_config|Exception: ', str(e))


def get_rule_id(context):
    rule_info = context['task_instance'].xcom_pull(task_ids='init_hec_single')['rule_info']
    if rule_info:
        rule_id = rule_info['id']
        print('get_rule_id|rule_id : ', rule_id)
        return rule_id
    else:
        return None


def set_running_status(dag_run, **kwargs):
    print('set_running_status|dag_run.conf : ', dag_run.conf)
    variable_routine = dag_run.conf
    variable_routine_id = variable_routine['id']
    if variable_routine_id is not None:
        update_workflow_status(2, variable_routine_id)
    else:
        print('set_running_status|variable_routine_id not found')


def set_complete_status(dag_run, **kwargs):
    print('set_complete_status')
    variable_routine = dag_run.conf
    variable_routine_id = variable_routine['id']
    update_workflow_status(3, variable_routine_id)


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run last_3_day_rain_dag DAG', catchup=False) as dag:
    init_task = PythonOperator(
        task_id='init_task',
        provide_context=True,
        python_callable=set_running_status,
        pool=dag_pool
    )

    task2 = DummyOperator(
        task_id='task2',
        pool=dag_pool
    )

    task3 = DummyOperator(
        task_id='task3',
        pool=dag_pool
    )

    task4 = DummyOperator(
        task_id='task4',
        pool=dag_pool
    )

    complete_state = PythonOperator(
        task_id='complete_state',
        provide_context=True,
        python_callable=set_complete_status,
        pool=dag_pool
    )

    init_task >> task2 >> task3 >> task4 >> complete_state
