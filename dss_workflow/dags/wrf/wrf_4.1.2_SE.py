from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators import GfsSensorOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import sys
import subprocess

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/dss_workflow/plugins/operators')
from gfs_sensor import GfsSensorOperator

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/accuracy_unit/wrf')
from wrf_accuracy import calculate_wrf_rule_accuracy

prod_dag_name = 'wrf_4.1.2_SE_dag'
dag_pool = 'wrf_pool'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

# ./runner.sh -r 0 -m SE -v 4.0 -h 18 -d 2019-10-24
# ./rfielder.sh -r 0 -m SE -v 4.0 -h 18

run_wrf4_SE_cmd_template = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@{} " \
                           "\'bash -c \"{}\"'"
rfield_gen_cmd = 'echo "rfield_gen_cmd" ;sleep $[($RANDOM % 100) + 1]s'
data_push_cmd = 'echo "data_push_cmd" ;sleep $[($RANDOM % 10) + 1]s'


def get_wrf_run_command(**context):
    wrf_rule = context['task_instance'].xcom_pull(task_ids='init_wrfv4SE')
    print('get_wrf_run_command|wrf_rule : ', wrf_rule)
    wrf_model = wrf_rule['model']
    wrf_version = wrf_rule['version']
    wrf_run = wrf_rule['rule_info']['run']
    gfs_hour = wrf_rule['rule_info']['hour']
    print('get_wrf_run_command|rule_details: ', wrf_rule['rule_info']['rule_details'])
    node_ip = wrf_rule['rule_info']['rule_details']['node_ip']
    script = wrf_rule['rule_info']['rule_details']['run_script']
    exec_date = context["execution_date"].to_datetime_string()
    run_script = '{}  -r {} -m {} -v {} -h {} -d {}'.format(script, wrf_run, wrf_model,
                                                            wrf_version, gfs_hour, exec_date)
    print('get_wrf_run_command|run_script : ', run_script)
    run_wrf4_SE_cmd = run_wrf4_SE_cmd_template.format(node_ip, run_script)
    print('get_wrf_run_command|run_wrf4_SE_cmd : ', run_wrf4_SE_cmd)
    subprocess.call(run_wrf4_SE_cmd, shell=True)


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
    rule_info = context['task_instance'].xcom_pull(task_ids='init_wrfv4SE')['rule_info']
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
    wrf_rule = {'model': 'SE', 'version': '4.1.2', 'rule_info': dag_run.conf}
    print('run_this_func|wrf_rule : ', wrf_rule)
    return wrf_rule


def check_accuracy(**context):
    print('check_accuracy|context : ', context)
    rule_info = context['task_instance'].xcom_pull(task_ids='init_wrfv4SE')['rule_info']
    print('check_accuracy|rule_info : ', rule_info)
    accuracy_rule_id = rule_info['accuracy_rule']
    if accuracy_rule_id == 0 or accuracy_rule_id == '0':
        return True
    else:
        wrf_rule = {'model': 'SE', 'version': '4.1.2', 'rule_info': rule_info}
        print('check_accuracy|wrf_rule : ', wrf_rule)
        exec_date = context["execution_date"].to_datetime_string()
        print('check_accuracy|exec_date : ', wrf_rule)
        return calculate_wrf_rule_accuracy(wrf_rule, exec_date)


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run WRF v4 SE DAG', catchup=False) as dag:
    init_wrfv4_SE = PythonOperator(
        task_id='init_wrfv4SE',
        provide_context=True,
        python_callable=run_this_func,
        pool=dag_pool
    )

    running_state_wrfv4SE = PythonOperator(
        task_id='running_state_wrfv4SE',
        provide_context=True,
        python_callable=set_running_status,
        pool=dag_pool
    )

    check_gfs_availability_wrfv4SE = GfsSensorOperator(
        task_id='check_gfs_availability_wrfv4SE',
        poke_interval=60,
        timeout=60 * 30,
        params={'model': 'SE', 'init_task_id': 'init_wrfv4SE'},
        provide_context=True,
        pool=dag_pool
    )

    run_wrf4_SE = PythonOperator(
        task_id='run_wrf4_SE',
        provide_context=True,
        python_callable=get_wrf_run_command,
        pool=dag_pool
    )

    rfield_gen_wrfv4SE = BashOperator(
        task_id='rfield_gen_wrfv4SE',
        bash_command=rfield_gen_cmd,
        pool=dag_pool
    )

    wrf_data_push_wrfv4SE = BashOperator(
        task_id='wrf_data_push_wrfv4SE',
        bash_command=data_push_cmd,
        pool=dag_pool
    )

    check_accuracy_wrfv4SE = PythonOperator(
        task_id='check_accuracy_wrfv4SE',
        provide_context=True,
        python_callable=check_accuracy,
        pool=dag_pool
    )

    complete_state_wrfv4SE = PythonOperator(
        task_id='complete_state_wrfv4SE',
        provide_context=True,
        python_callable=set_complete_status,
        pool=dag_pool
    )

    init_wrfv4_SE >> running_state_wrfv4SE >> check_gfs_availability_wrfv4SE >> \
    run_wrf4_SE >> rfield_gen_wrfv4SE >> wrf_data_push_wrfv4SE >> \
    check_accuracy_wrfv4SE >> complete_state_wrfv4SE
