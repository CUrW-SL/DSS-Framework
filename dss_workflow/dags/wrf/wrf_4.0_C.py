from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#from airflow.operators import GfsSensorOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import sys

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/dss_workflow/plugins/operators')
from gfs_sensor import GfsSensorOperator

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

prod_dag_name = 'wrf_4.0_C_dag'
dag_pool = 'wrf_pool'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

run_wrf4_C_cmd = 'echo "run_wrf_C_cmd" ;sleep $[($RANDOM % 1000) + 1]s'
rfield_gen_cmd = 'echo "rfield_gen_cmd" ;sleep $[($RANDOM % 100) + 1]s'
data_push_cmd = 'echo "data_push_cmd" ;sleep $[($RANDOM % 10) + 1]s'


def update_workflow_status(status, rule_id):
    try:
        db_config = Variable.get('db_config', deserialize_json=True)
        try:
            adapter = RuleEngineAdapter.get_instance(db_config)
            adapter.update_rule_status_by_id('wrf', rule_id, status)
        except Exception as ex:
            print('update_workflow_status|db_adapter|Exception: ', str(ex))
    except Exception as e:
        print('update_workflow_status|db_config|Exception: ', str(e))


def get_rule_id(context):
    rule_info = context['task_instance'].xcom_pull(task_ids='init_wrfv4C')['rule_info']
    if rule_info:
        rule_id = rule_info['id']
        print('get_rule_id|rule_id : ', rule_id)
        return rule_id
    else:
        return None


def set_running_status(**context):
    rule_id = get_rule_id(context)
    if rule_id is not None:
        update_workflow_status(2, rule_id)
    else:
        print('set_running_status|rule_id not found')


def set_complete_status(**context):
    rule_id = get_rule_id(context)
    if rule_id is not None:
        update_workflow_status(3, rule_id)
    else:
        print('set_complete_status|rule_id not found')


def run_this_func(dag_run, **kwargs):
    print('run_this_func|dag_run : ', dag_run)
    wrf_rule = {'model': 'C', 'version': '4.0', 'rule_info': dag_run.conf}
    print('run_this_func|wrf_rule : ', wrf_rule)
    return wrf_rule


def check_accuracy(**context):
    print('check_accuracy|context : ', context)
    rule_info = context['task_instance'].xcom_pull(task_ids='init_wrfv4C')['rule_info']
    print('check_accuracy|rule_info : ', rule_info)
    wrf_rule = {'model': 'C', 'version': '4.0', 'rule_info': rule_info}
    print('check_accuracy|wrf_rule : ', wrf_rule)


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run WRF v4 C DAG', catchup=False) as dag:
    init_wrfv4_C = PythonOperator(
        task_id='init_wrfv4C',
        provide_context=True,
        python_callable=run_this_func,
        pool=dag_pool
    )

    running_state_wrfv4C = PythonOperator(
        task_id='running_state_wrfv4C',
        provide_context=True,
        python_callable=set_running_status,
        pool=dag_pool
    )

    check_gfs_availability_wrfv4C = GfsSensorOperator(
        task_id='check_gfs_availability_wrfv4C',
        poke_interval=60,
        timeout=60 * 30,
        params={'model': 'C', 'init_task_id': 'init_wrfv4C'},
        provide_context=True,
        pool=dag_pool
    )

    run_wrf4_C = BashOperator(
        task_id='run_wrf4_C',
        bash_command=run_wrf4_C_cmd,
        pool=dag_pool
    )

    rfield_gen_wrfv4C = BashOperator(
        task_id='rfield_gen_wrfv4C',
        bash_command=rfield_gen_cmd,
        pool=dag_pool
    )

    wrf_data_push_wrfv4C = BashOperator(
        task_id='wrf_data_push_wrfv4C',
        bash_command=data_push_cmd,
        pool=dag_pool
    )

    check_accuracy_wrfv4C = PythonOperator(
        task_id='check_accuracy_wrfv4C',
        provide_context=True,
        python_callable=check_accuracy,
        pool=dag_pool
    )

    complete_state_wrfv4C = PythonOperator(
        task_id='complete_state_wrfv4C',
        provide_context=True,
        python_callable=set_complete_status,
        pool=dag_pool
    )

    init_wrfv4_C >> running_state_wrfv4C >> check_gfs_availability_wrfv4C >> \
    run_wrf4_C >> rfield_gen_wrfv4C >> wrf_data_push_wrfv4C >> \
    check_accuracy_wrfv4C >> complete_state_wrfv4C

