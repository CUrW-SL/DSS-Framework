from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import sys

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/dss_workflow/plugins/operators')
from workflow_completion_sensor import WorkflowSensorOperator

prod_dag_name = 'flo2d_250m_dag'
dag_pool = 'flo2d_pool'


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7gmail.com'],
    'email_on_failure': True,
}

create_raincell_cmd = 'echo "create_raincell_cmd" ;sleep $[($RANDOM % 10) + 1]s'
create_inflow_cmd = 'echo "create_inflow_cmd" ;sleep $[($RANDOM % 10) + 1]s'
create_outflow_cmd = 'echo "create_outflow_cmd" ;sleep $[($RANDOM % 10) + 1]s'
run_flo2d_250m_cmd = 'echo "run_flo2d_250m_cmd" ;sleep 900s'
extract_water_level_cmd = 'echo "extract_water_level_cmd" ;sleep $[($RANDOM % 10) + 1]s'


def update_workflow_status(status, rule_id):
    try:
        db_config = Variable.get('db_config', deserialize_json=True)
        try:
            adapter = RuleEngineAdapter.get_instance(db_config)
            adapter.update_rule_status_by_id('flo2d', rule_id, status)
        except Exception as ex:
            print('update_workflow_status|db_adapter|Exception: ', str(ex))
    except Exception as e:
        print('update_workflow_status|db_config|Exception: ', str(e))


def get_rule_id(context):
    rule_info = context['task_instance'].xcom_pull(task_ids='init_flo2d_250m')['rule_info']
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
    flo2d_rule = {'model': '250m', 'rule_info': dag_run.conf}
    print('run_this_func|flo2d_rule : ', flo2d_rule)
    return flo2d_rule


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run Flo2d 250m DAG', catchup=False) as dag:

    init_flo2d_250m = PythonOperator(
        task_id='init_flo2d_250m',
        provide_context=True,
        python_callable=run_this_func,
        pool=dag_pool
    )

    running_state_flo2d_250m = PythonOperator(
        task_id='running_state_flo2d_250m',
        provide_context=True,
        python_callable=set_running_status,
        pool=dag_pool
    )

    check_wrf_completion_flo2d_250m = WorkflowSensorOperator(
        task_id='check_wrf_completion_flo2d_250m',
        poke_interval=60,
        timeout=3600,
        params={'model': 'wrf', 'init_task_id': 'init_flo2d_250m'},
        provide_context=True
     )

    create_raincell_flo2d_250m = BashOperator(
        task_id='create_raincell_flo2d_250m',
        bash_command=create_raincell_cmd,
        pool=dag_pool
    )

    check_hechms_completion_flo2d_250m = WorkflowSensorOperator(
        task_id='check_hechms_completion_flo2d_250m',
        poke_interval=60,
        timeout=1800,
        params={'model': 'hechms', 'init_task_id': 'init_flo2d_250m'},
        provide_context=True
     )

    create_inflow_flo2d_250m = BashOperator(
        task_id='create_inflow_flo2d_250m',
        bash_command=create_inflow_cmd,
        pool=dag_pool
    )

    create_outflow_flo2d_250m = BashOperator(
        task_id='create_outflow_flo2d_250m',
        bash_command=create_outflow_cmd,
        pool=dag_pool
    )

    run_flo2d_250m_flo2d_250m = BashOperator(
        task_id='run_flo2d_250m_flo2d_250m',
        bash_command=run_flo2d_250m_cmd,
        pool=dag_pool
    )

    extract_water_level_flo2d_250m = BashOperator(
        task_id='extract_water_level_flo2d_250m',
        bash_command=extract_water_level_cmd,
        pool=dag_pool
    )

    complete_state_flo2d_250m = PythonOperator(
        task_id='complete_state_flo2d_250m',
        provide_context=True,
        python_callable=set_complete_status,
        pool=dag_pool
    )

    init_flo2d_250m >> running_state_flo2d_250m >> check_wrf_completion_flo2d_250m >> create_raincell_flo2d_250m >> \
    check_hechms_completion_flo2d_250m >> create_inflow_flo2d_250m >> create_outflow_flo2d_250m >> run_flo2d_250m_flo2d_250m >> \
    extract_water_level_flo2d_250m >> complete_state_flo2d_250m

