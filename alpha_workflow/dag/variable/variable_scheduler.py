from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import sys

from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/plugins/operators')
from dynamic_external_trigger_operator import DynamicTriggerDagRunOperator

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/utils')
from db_util import RuleEngineAdapter

sys.path.insert(0, '/home/curw/git/DSS-Framework/gen_util')
from controller_util import get_triggering_variable_dags

prod_dag_name = 'variable_scheduler_v1'
schedule_interval = '*/5 * * * *'
dag_pool = 'variable_scheduler_pool'


def generate_dag_run(context):
    print('***************************init_variable_routine**********************************')
    db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    run_date = context["execution_date"].to_datetime_string()
    print('init_variable_routine|run_date : ', run_date)
    routines = adapter.get_next_variable_routines(run_date)
    print('init_variable_routine|routines : ', routines)
    next_variable_routines = []
    if routines is not None:
        next_variable_routines = get_triggering_variable_dags(routines)
    else:
        print('No variable routine to schedule.')
    return next_variable_routines


def end_routine():
    print('******rounting has completed**********')


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.strptime('2020-05-25 01:00:00', '%Y-%m-%d %H:%M:%S'),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(dag_id=prod_dag_name, default_args=default_args,
         catchup=False, schedule_interval=schedule_interval,
         description='Run DSS Controller DAG') as dag:
    scheduler_init = DummyOperator(
        task_id='scheduler_init',
        pool=dag_pool
    )

    gen_target_dag_run = DynamicTriggerDagRunOperator(
        task_id='gen_target_dag_run',
        default_trigger='variable_scheduler_v1',
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
