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

prod_dag_name = 'hechms_single_dag'
dag_pool = 'hechms_pool'


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

create_rainfall_cmd = 'echo "create_rainfall_cmd" ;sleep $[($RANDOM % 10) + 1]s'
run_hechms_single_cmd = 'echo "run_hechms_single_cmd" ;sleep $[($RANDOM % 10) + 1]s'
upload_discharge_cmd = 'echo "upload_discharge_cmd" ;sleep $[($RANDOM % 10) + 1]s'


def update_workflow_status(status, rule_id):
    try:
        db_config = Variable.get('db_config', deserialize_json=True)
        try:
            adapter = RuleEngineAdapter.get_instance(db_config)
            adapter.update_rule_status_by_id('hechms', rule_id, status)
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
    hechms_rule = {'model': 'single', 'rule_info': dag_run.conf}
    print('run_this_func|hechms_rule : ', hechms_rule)
    return hechms_rule


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run HecHms DAG', catchup=False) as dag:
    init_hec_single = PythonOperator(
        task_id='init_hec_single',
        provide_context=True,
        python_callable=run_this_func,
        pool=dag_pool
    )

    running_state_hec_sin = PythonOperator(
        task_id='running_state_hec_sin',
        provide_context=True,
        python_callable=set_running_status,
        dag=dag,
        pool=dag_pool
    )

    check_wrf_completion_hec_sin = WorkflowSensorOperator(
        task_id='check_wrf_completion_hec_sin',
        poke_interval=60,
        timeout=60 * 6 * 60,
        params={'model': 'wrf', 'init_task_id': 'init_hec_single'},
        provide_context=True,
        dag=dag)

    create_rainfall_hec_sin = BashOperator(
        task_id='create_rainfall_hec_sin',
        bash_command=create_rainfall_cmd,
        pool=dag_pool
    )

    run_hechms_single = BashOperator(
        task_id='run_hechms_single',
        bash_command=run_hechms_single_cmd,
        pool=dag_pool
    )

    upload_discharge_hec_sin = BashOperator(
        task_id='upload_discharge_hec_sin',
        bash_command=upload_discharge_cmd,
        pool=dag_pool
    )

    complete_state_hec_sin = PythonOperator(
        task_id='complete_state_hec_sin',
        provide_context=True,
        python_callable=set_complete_status,
        dag=dag,
        pool=dag_pool
    )

    init_hec_single >> running_state_hec_sin >> check_wrf_completion_hec_sin\
    >> create_rainfall_hec_sin >> run_hechms_single >> upload_discharge_hec_sin\
    >> complete_state_hec_sin

