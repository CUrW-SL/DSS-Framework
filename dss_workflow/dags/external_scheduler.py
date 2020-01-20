from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import sys

from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/dss_workflow/plugins/operators')
from dynamic_external_trigger_operator import DynamicTriggerDagRunOperator

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/gen_util')
from controller_util import get_triggering_external_bash_dags

prod_dag_name = 'external_scheduler_v1'
schedule_interval = '*/5 * * * *'
dag_pool = 'external_scheduler_pool'


def generate_dag_run(context):
    print('***************************init_variable_routine**********************************')
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    run_date = context["execution_date"].to_datetime_string()
    print('init_variable_routine|run_date : ', run_date)
    routines = adapter.get_external_bash_routines(run_date)
    print('init_variable_routine|routines : ', routines)
    next_variable_routines = []
    if routines is not None:
        next_variable_routines = get_triggering_external_bash_dags(routines)
    else:
        print('No variable routine to schedule.')
    return next_variable_routines


def end_routine():
    print('******rounting has completed**********')


def get_rule_id(context):
    rule_info = context['task_instance'].xcom_pull(task_ids='init_flo2d_150m')['rule_info']
    if rule_info:
        rule_id = rule_info['id']
        print('get_rule_id|rule_id : ', rule_id)
        return rule_id
    else:
        return None


def update_workflow_status(status, rule_id):
    try:
        db_config = Variable.get('db_config', deserialize_json=True)
        try:
            adapter = RuleEngineAdapter.get_instance(db_config)
            adapter.update_initial_external_bash_routing_status(status, rule_id)
        except Exception as ex:
            print('update_workflow_status|db_adapter|Exception: ', str(ex))
    except Exception as e:
        print('update_workflow_status|db_config|Exception: ', str(e))


def on_dag_failure(context):
    rule_id = get_rule_id(context)
    if rule_id is not None:
        update_workflow_status(4, rule_id)
        print('on_dag_failure|set error status for rule|rule_id :', rule_id)
    else:
        print('on_dag_failure|rule_id not found')


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.strptime('2020-01-20 04:00:00', '%Y-%m-%d %H:%M:%S'),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(dag_id=prod_dag_name, default_args=default_args,
         catchup=False, schedule_interval=schedule_interval,
         description='Run External Scheduler DAG') as dag:
    scheduler_init = DummyOperator(
        task_id='scheduler_init',
        pool=dag_pool
    )

    gen_target_dag_run = DynamicTriggerDagRunOperator(
        task_id='gen_target_dag_run',
        default_trigger='dss_variable_routine_v1',
        python_callable=generate_dag_run,
        pool=dag_pool

    )

    scheduler_end = PythonOperator(
        task_id='scheduler_end',
        python_callable=end_routine,
        trigger_rule='none_failed',
        pool=dag_pool
    )

    scheduler_init >> gen_target_dag_run >> scheduler_end
