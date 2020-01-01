from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import sys
import subprocess

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/accuracy_unit/flo2d')
from flo2d_accuracy import calculate_flo2d_rule_accuracy

prod_dag_name = 'flo2d_250m_dag'
dag_pool = 'flo2d_pool'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7gmail.com'],
    'email_on_failure': True,
}

create_raincell_cmd_template = 'curl -X GET "http://{}:{}/create-sim-raincell?' \
                               'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                               '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}' \
                               '&forward={}&backward={}"'
create_inflow_cmd_template = 'curl -X GET "http://{}:{}/create-inflow?' \
                             'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                             '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}"'
create_outflow_cmd_template = 'curl -X GET "http://{}:{}/create-outflow?' \
                              'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                              '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}' \
                              '&forward={}&backward={}"'
run_flo2d_250m_cmd_template = 'curl -X GET "http://{}:{}/run-flo2d?' \
                              'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                              '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}"'
extract_water_level_cmd_template = 'curl -X GET "http://{}:{}/extract-data?' \
                                   'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                                   '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}"'


def get_rule_from_context(context):
    rule = context['task_instance'].xcom_pull(task_ids='init_flo2d_250m')
    print('get_rule_from_context|rule : ', rule)
    return rule


def get_create_raincell_cmd(**context):
    rule = get_rule_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    create_raincell_cmd = create_raincell_cmd_template.format(run_node, run_port, forward, backward)
    print('get_create_raincell_cmd|create_raincell_cmd : ', create_raincell_cmd)
    subprocess.call(create_raincell_cmd, shell=True)


def get_create_inflow_cmd(**context):
    rule = get_rule_from_context(context)
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    create_inflow_cmd = create_inflow_cmd_template.format(run_node, run_port)
    print('get_create_inflow_cmd|create_inflow_cmd : ', create_inflow_cmd)
    subprocess.call(create_inflow_cmd, shell=True)


def get_create_outflow_cmd(**context):
    rule = get_rule_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    create_outflow_cmd = create_outflow_cmd_template.format(run_node, run_port, forward, backward)
    print('get_create_outflow_cmd|create_outflow_cmd : ', create_outflow_cmd)
    subprocess.call(create_outflow_cmd, shell=True)


def get_run_flo2d_250m_cmd(**context):
    rule = get_rule_from_context(context)
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    run_flo2d_250m_cmd = run_flo2d_250m_cmd_template.format(run_node, run_port)
    print('get_create_inflow_cmd|run_flo2d_250m_cmd : ', run_flo2d_250m_cmd)
    subprocess.call(run_flo2d_250m_cmd, shell=True)


def get_extract_water_level_cmd(**context):
    rule = get_rule_from_context(context)
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    extract_water_level_cmd = extract_water_level_cmd_template.format(run_node, run_port)
    print('get_create_inflow_cmd|extract_water_level_cmd : ', extract_water_level_cmd)
    subprocess.call(extract_water_level_cmd, shell=True)


def check_accuracy(**context):
    print('check_accuracy|context : ', context)
    rule_info = context['task_instance'].xcom_pull(task_ids='init_flo2d_250m')['rule_info']
    print('check_accuracy|rule_info : ', rule_info)
    flo2d_rule = {'model': 'FLO2D', 'version': '250', 'rule_info': rule_info}
    print('check_accuracy|flo2d_rule : ', flo2d_rule)
    exec_date = context["execution_date"].to_datetime_string()
    print('check_accuracy|exec_date : ', flo2d_rule)
    # TODO: condition tobe added
    calculate_flo2d_rule_accuracy(flo2d_rule, exec_date)


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

    create_raincell_flo2d_250m = PythonOperator(
        task_id='create_raincell_flo2d_250m',
        provide_context=True,
        python_callable=get_create_raincell_cmd,
        pool=dag_pool
    )

    create_inflow_flo2d_250m = PythonOperator(
        task_id='create_raincell_flo2d_250m',
        provide_context=True,
        python_callable=get_create_inflow_cmd,
        pool=dag_pool
    )

    create_outflow_flo2d_250m = PythonOperator(
        task_id='create_raincell_flo2d_250m',
        provide_context=True,
        python_callable=get_create_outflow_cmd,
        pool=dag_pool
    )

    run_flo2d_250m_flo2d_250m = PythonOperator(
        task_id='create_raincell_flo2d_250m',
        provide_context=True,
        python_callable=get_run_flo2d_250m_cmd,
        pool=dag_pool
    )

    extract_water_level_flo2d_250m = PythonOperator(
        task_id='create_raincell_flo2d_250m',
        provide_context=True,
        python_callable=get_extract_water_level_cmd,
        pool=dag_pool
    )

    check_accuracy_flo2d250m = PythonOperator(
        task_id='check_accuracy_flo2d250m',
        provide_context=True,
        python_callable=check_accuracy,
        pool=dag_pool
    )

    complete_state_flo2d_250m = PythonOperator(
        task_id='complete_state_flo2d_250m',
        provide_context=True,
        python_callable=set_complete_status,
        pool=dag_pool
    )

    init_flo2d_250m >> running_state_flo2d_250m >> create_raincell_flo2d_250m >> \
    create_inflow_flo2d_250m >> create_outflow_flo2d_250m >> run_flo2d_250m_flo2d_250m >> \
    extract_water_level_flo2d_250m >> check_accuracy_flo2d250m >> complete_state_flo2d_250m
