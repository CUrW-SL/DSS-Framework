from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import sys
import subprocess

sys.path.insert(0, '/home/curw/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

prod_dag_name = 'hechms_distributed_dag'
dag_pool = 'hechms_pool'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

create_input_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/init/{}/{}/{}/{}"'

run_hechms_preprocess_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/pre-process/{}/{}/{}"'

run_hechms_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/run"'

run_hechms_postprocess_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/post-process/{}/{}/{}"'

upload_discharge_cmd_template = 'curl -X GET "http://10.138.0.3:5000/HECHMS/distributed/upload-discharge/{}"'


def get_rule_from_context(context):
    rule = context['task_instance'].xcom_pull(task_ids='init_hec_distributed')
    print('get_rule_from_context|rule : ', rule)
    return rule


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
    rule_info = context['task_instance'].xcom_pull(task_ids='init_hec_distributed')['rule_info']
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
    hechms_rule = {'model': 'distributed', 'rule_info': dag_run.conf}
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
         description='Run HecHms DAG', catchup=False, on_failure_callback=on_dag_failure) as dag:
    init_hec_distributed = PythonOperator(
        task_id='init_hec_distributed',
        provide_context=True,
        python_callable=run_this_func,
        pool=dag_pool
    )

    running_state_hec_dis = PythonOperator(
        task_id='running_state_hec_dis',
        provide_context=True,
        python_callable=set_running_status,
        dag=dag,
        pool=dag_pool
    )

    create_input_hec_dis = PythonOperator(
        task_id='create_input_hec_dis',
        provide_context=True,
        python_callable=get_create_input_cmd,
        pool=dag_pool
    )

    run_hechms_preprocess_hec_dis = PythonOperator(
        task_id='run_hechms_preprocess_hec_dis',
        provide_context=True,
        python_callable=get_run_hechms_preprocess_cmd,
        pool=dag_pool
    )

    run_hechms_hec_dis = PythonOperator(
        task_id='run_hechms_hec_dis',
        provide_context=True,
        python_callable=get_run_hechms_cmd,
        pool=dag_pool
    )

    run_hechms_postprocess_hec_dis = PythonOperator(
        task_id='run_hechms_postprocess_hec_dis',
        provide_context=True,
        python_callable=get_run_hechms_postprocess_cmd,
        pool=dag_pool
    )

    upload_discharge_hec_dis = PythonOperator(
        task_id='upload_discharge_hec_dis',
        provide_context=True,
        python_callable=get_upload_discharge_cmd,
        pool=dag_pool
    )

    complete_state_hec_dis = PythonOperator(
        task_id='complete_state_hec_dis',
        provide_context=True,
        python_callable=set_complete_status,
        dag=dag,
        pool=dag_pool
    )

    init_hec_distributed >> running_state_hec_dis >> create_input_hec_dis >> run_hechms_preprocess_hec_dis >> \
    run_hechms_hec_dis >> run_hechms_postprocess_hec_dis >> upload_discharge_hec_dis >> complete_state_hec_dis
