import sys
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from gen_db import CurwObsAdapter
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/variable_util')
from current_rainfall import update_current_rainfall_values

prod_dag_name = 'current_rainfall_dag'
dag_pool = 'rain_intensity_pool'


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


def update_variable_value(dag_run, **kwargs):
    print('update_variable_value')
    variable_routine = dag_run.conf
    db_config = Variable.get('db_config', deserialize_json=True)
    obs_db_config = Variable.get('obs_db_config', deserialize_json=True)
    try:
        dss_adapter = RuleEngineAdapter.get_instance(db_config)
        obs_adapter = CurwObsAdapter.get_instance(obs_db_config)
        print('update_variable_value|variable_routine : ', variable_routine)
        update_current_rainfall_values(dss_adapter, obs_adapter, variable_routine)
    except Exception as ex:
        print('update_variable_value|db_adapter|Exception: ', str(ex))


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run current_rainfall_intensity_dag DAG', catchup=False) as dag:
    init_task = PythonOperator(
        task_id='init_task',
        provide_context=True,
        python_callable=set_running_status,
        pool=dag_pool
    )

    update_variable_value = PythonOperator(
        task_id='update_variable_value',
        provide_context=True,
        python_callable=update_variable_value,
        pool=dag_pool
    )

    complete_state = PythonOperator(
        task_id='complete_state',
        provide_context=True,
        python_callable=set_complete_status,
        pool=dag_pool
    )

    init_task >> update_variable_value >> complete_state

