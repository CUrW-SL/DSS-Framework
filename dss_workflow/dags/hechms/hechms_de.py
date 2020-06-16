from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import sys
import subprocess

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

RUN_SCRIPT = '/home/uwcc-admin/git/DSS-Framework/docker/hechms/runner.sh'

prod_dag_name = 'hechms_distributed_event_dag'
dag_pool = 'hechms_pool'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

ssh_cmd_template = 'sshpass -p \'{}\' ssh {}@{} {}'

ssh_cmd_template = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@{ip} " \
                   "\'bash -c \"{run script}\"'"

create_input_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/init/{}/{}/{}/{}"'

run_hechms_preprocess_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/pre-process/{}/{}/{}"'

run_hechms_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/run"'

run_hechms_postprocess_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/post-process/{}/{}/{}"'

upload_discharge_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/upload-discharge/{}"'


def get_rule_from_context(context):
    rule = context['task_instance'].xcom_pull(task_ids='init_hechms')
    print('get_rule_from_context|rule : ', rule)
    return rule


def run_hechms_workflow(**context):
    [exec_date, date_only, time_only] = get_local_exec_date_from_context(context)
    rule_id = get_rule_id(context)
    print('run_hechms|[exec_date,date_only, time_only, rule_id] : ', [exec_date, date_only, time_only, rule_id])
    vm_config = Variable.get('ubuntu1_config', deserialize_json=True)
    vm_user = vm_config['user']
    vm_password = vm_config['password']
    rule = context['task_instance'].xcom_pull(task_ids='init_hechms')
    if rule is not None:
        forward = rule['forecast_days']
        backward = rule['observed_days']
        init_run = rule['init_run']
        target_model = rule['target_model']
        pop_method = rule['rainfall_data_from']
        run_node = rule['rule_details']['run_node']
        run_type = 'production'
        db_config = Variable.get('prod_db_config', deserialize_json=True)['sim_config']
        run_script = '{}  -d {} -f {} -b {} -r {} -p {} -D {} -T {} -u {} -x {} -y {} -z {} -m {} -n {}'.format(
            RUN_SCRIPT,
            exec_date,
            forward, backward,
            init_run, pop_method,
            date_only, time_only, db_config['mysql_user'],
            db_config['mysql_password'],
            db_config['mysql_host'],
            db_config['mysql_db'],
            target_model, run_type)
        print('get_wrf_run_command|run_script : ', run_script)
        run_wrf_cmd = ssh_cmd_template.format(vm_password, vm_user, run_node, run_script)
        print('get_wrf_run_command|run_wrf_cmd : ', run_wrf_cmd)
        print('get_wrf_run_command|run_wrf_cmd : ', run_wrf_cmd)
        subprocess.call(run_wrf_cmd, shell=True)
    else:
        raise AirflowException('hechms rule not found')


def get_local_exec_date_from_context(context):
    exec_datetime_str = context["execution_date"].to_datetime_string()
    exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S') + timedelta(hours=5, minutes=30)
    exec_date = exec_datetime.strftime('%Y-%m-%d_%H:00:00')
    return exec_date


def get_create_input_cmd(**context):
    rule = get_rule_from_context(context)
    exec_date = get_local_exec_date_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    init_run = rule['rule_info']['init_run']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    create_input_cmd = create_input_cmd_template.format(run_node, run_port, exec_date, backward, forward, init_run)
    print('get_create_input_cmd|create_input_cmd : ', create_input_cmd)
    subprocess.call(create_input_cmd, shell=True)


def get_run_hechms_preprocess_cmd(**context):
    rule = get_rule_from_context(context)
    exec_date = get_local_exec_date_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    run_hechms_preprocess_cmd = run_hechms_preprocess_cmd_template.format(run_node, run_port, exec_date, backward,
                                                                          forward)
    print('get_run_hechms_preprocess_cmd|run_hechms_preprocess_cmd : ', run_hechms_preprocess_cmd)
    subprocess.call(run_hechms_preprocess_cmd, shell=True)


def get_run_hechms_cmd(**context):
    rule = get_rule_from_context(context)
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    run_hechms_cmd = run_hechms_cmd_template.format(run_node, run_port)
    print('get_run_hechms_preprocess_cmd|run_hechms_cmd : ', run_hechms_cmd)
    subprocess.call(run_hechms_cmd, shell=True)


def get_run_hechms_postprocess_cmd(**context):
    rule = get_rule_from_context(context)
    exec_date = get_local_exec_date_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    run_hechms_postprocess_cmd = run_hechms_postprocess_cmd_template.format(run_node, run_port, exec_date, backward,
                                                                            forward)
    print('get_run_hechms_postprocess_cmd|run_hechms_postprocess_cmd : ', run_hechms_postprocess_cmd)
    subprocess.call(run_hechms_postprocess_cmd, shell=True)


def get_upload_discharge_cmd(**context):
    rule = get_rule_from_context(context)
    exec_date = get_local_exec_date_from_context(context)
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    upload_discharge_cmd = upload_discharge_cmd_template.format(run_node, run_port, exec_date)
    print('get_upload_discharge_cmd|upload_discharge_cmd : ', upload_discharge_cmd)
    subprocess.call(upload_discharge_cmd, shell=True)


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
    rule_info = context['task_instance'].xcom_pull(task_ids='init_hechms')['rule_info']
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
    hechms_rule = dag_run.conf
    print('run_this_func|hechms_rule : ', hechms_rule)
    return hechms_rule


def on_dag_failure(context):
    rule_id = get_rule_id(context)
    if rule_id is not None:
        update_workflow_status(4, rule_id)
        print('on_dag_failure|set error status for rule|rule_id :', rule_id)
    else:
        print('on_dag_failure|rule_id not found')


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run HecHms DISTRIBUTED EVENT DAG', catchup=False, on_failure_callback=on_dag_failure) as dag:
    init_hechms = PythonOperator(
        task_id='init_hechms',
        provide_context=True,
        python_callable=run_this_func,
        pool=dag_pool
    )

    running_state = PythonOperator(
        task_id='running_state',
        provide_context=True,
        python_callable=set_running_status,
        dag=dag,
        pool=dag_pool
    )

    run_hechms = PythonOperator(
        task_id='run_hechms',
        provide_context=True,
        python_callable=run_hechms_workflow,
        pool=dag_pool
    )

    complete_state = PythonOperator(
        task_id='complete_state',
        provide_context=True,
        python_callable=set_complete_status,
        dag=dag,
        pool=dag_pool
    )

    init_hechms >> running_state >> run_hechms >> complete_state