from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import sys
import subprocess
import requests

sys.path.insert(0, '/home/curw/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

prod_dag_name = 'hechms_distributed_dag'
dag_pool = 'hechms_pool'
git_path = '/home/curw/git'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

create_input_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/init/{}/{}/{}/{}/{}"'
create_input_request = 'http://{}:{}/HECHMS/distributed/init/{}/{}/{}/{}/{}'

run_hechms_preprocess_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/pre-process/{}/{}/{}"'
run_hechms_preprocess_request = 'http://{}:{}/HECHMS/distributed/pre-process/{}/{}/{}'

run_hechms_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/run"'
run_hechms_cmd_request = 'http://{}:{}/HECHMS/distributed/run'

run_hechms_postprocess_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/post-process/{}/{}/{}"'
run_hechms_postprocess_request = 'http://{}:{}/HECHMS/distributed/post-process/{}/{}/{}'

upload_discharge_cmd_template = 'curl -X GET "http://{}:{}/HECHMS/distributed/upload-discharge/{}"'
upload_discharge_cmd_request = 'http://{}:{}/HECHMS/distributed/upload-discharge/{}/{}'


def send_http_get_request(url, params=None):
    if params is not None:
        r = requests.get(url=url, params=params)
    else:
        r = requests.get(url=url)
    response = r.json()
    print('send_http_get_request|response : ', response)
    if response == {'Result': 'Success'}:
        return True
    return False


def get_rule_from_context(context):
    rule = context['task_instance'].xcom_pull(task_ids='init_hec_distributed')
    print('get_rule_from_context|rule : ', rule)
    return rule


def get_local_exec_date_from_context(context):
    rule = get_rule_from_context(context)
    if 'run_date' in rule['rule_info']:
        exec_datetime_str = rule['rule_info']['run_date']
        exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S')
        exec_date = exec_datetime.strftime('%Y-%m-%d_%H:00:00')
    else:
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
    pop_method = rule['rule_info']['rainfall_data_from']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    create_input_cmd = create_input_cmd_template.format(run_node, run_port, exec_date, backward, forward, init_run,
                                                        pop_method)
    print('get_create_input_cmd|create_input_cmd : ', create_input_cmd)
    # subprocess.call(create_input_cmd, shell=True)
    request_url = create_input_request.format(run_node, run_port, exec_date, backward, forward, init_run, pop_method)
    print('get_create_input_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_create_input_cmd|success')
    else:
        raise AirflowException(
            'get_create_input_cmd|failed'
        )


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
    #subprocess.call(run_hechms_preprocess_cmd, shell=True)
    request_url = run_hechms_preprocess_request.format(run_node, run_port, exec_date, backward, forward)
    print('get_run_hechms_preprocess_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_run_hechms_preprocess_cmd|success')
    else:
        raise AirflowException(
            'get_run_hechms_preprocess_cmd|failed'
        )


def get_run_hechms_cmd(**context):
    rule = get_rule_from_context(context)
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    run_hechms_cmd = run_hechms_cmd_template.format(run_node, run_port)
    print('get_run_hechms_preprocess_cmd|run_hechms_cmd : ', run_hechms_cmd)
    #subprocess.call(run_hechms_cmd, shell=True)
    request_url = run_hechms_cmd_request.format(run_node, run_port)
    print('get_run_hechms_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_run_hechms_cmd|success')
    else:
        raise AirflowException(
            'get_run_hechms_cmd|failed'
        )


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
    #subprocess.call(run_hechms_postprocess_cmd, shell=True)
    request_url = run_hechms_postprocess_request.format(run_node, run_port, exec_date, backward, forward)
    print('get_run_hechms_postprocess_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_run_hechms_postprocess_cmd|success')
    else:
        raise AirflowException(
            'get_run_hechms_postprocess_cmd|failed'
        )


#upload_discharge_cmd_template = './home/curw/git/output/extract_hechms_discharge.py -m hechms_{} ' \
        # '-s "{YYYY-MM-DD HH:MM:SS}" -r "{YYYY-MM-DD HH:MM:SS}" ' \
        # '-t event_run -d "{}"'


def get_upload_discharge_cmd(**context):
    rule = get_rule_from_context(context)
    exec_date = get_local_exec_date_from_context(context)
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    target_model = rule['rule_info']['target_model']
    upload_discharge_cmd = upload_discharge_cmd_template.format(run_node, run_port, exec_date)
    print('get_upload_discharge_cmd|upload_discharge_cmd : ', upload_discharge_cmd)
    request_url = upload_discharge_cmd_request.format(run_node, run_port, exec_date, target_model)
    print('get_upload_discharge_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_upload_discharge_cmd|success')
    else:
        raise AirflowException(
            'get_upload_discharge_cmd|failed'
        )


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
        task_id='complete_state',
        provide_context=True,
        python_callable=set_complete_status,
        dag=dag,
        pool=dag_pool
    )

    init_hec_distributed >> running_state_hec_dis >> create_input_hec_dis >> run_hechms_preprocess_hec_dis >> \
    run_hechms_hec_dis >> run_hechms_postprocess_hec_dis >> upload_discharge_hec_dis >> complete_state_hec_dis
