from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import sys
import requests

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/utils')
from db_util import RuleEngineAdapter

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


def get_local_exec_date_from_context(context):
    rule = context['task_instance'].xcom_pull(task_ids='init_hechms')
    if 'run_date' in rule:
        exec_datetime_str = rule['run_date']
        exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S')
        exec_date = exec_datetime.strftime('%Y-%m-%d_%H:00:00')
    else:
        exec_datetime_str = context["execution_date"].to_datetime_string()
        exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S') + timedelta(hours=5, minutes=30)
        exec_date = exec_datetime.strftime('%Y-%m-%d_%H:00:00')
    return exec_date


def get_create_input_cmd(**context):
    exec_date = get_local_exec_date_from_context(context)
    rule_id = get_rule_id(context)
    allowed_to_proceed(rule_id)
    rule = get_rule_by_id(rule_id)
    if rule is not None:
        forward = rule['forecast_days']
        backward = rule['observed_days']
        init_run = rule['init_run']
        pop_method = rule['rainfall_data_from']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        create_input_cmd = create_input_cmd_template.format(run_node, run_port, exec_date, backward, forward, init_run,
                                                            pop_method)
        print('get_create_input_cmd|create_input_cmd : ', create_input_cmd)
        request_url = create_input_request.format(run_node, run_port, exec_date, backward, forward, init_run, pop_method)
        print('get_create_input_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_create_input_cmd|success')
        else:
            raise AirflowException(
                'get_create_input_cmd|failed'
            )
    else:
        raise AirflowException(
            'get_create_input_cmd|hechms rule not found'
        )


def get_run_hechms_preprocess_cmd(**context):
    exec_date = get_local_exec_date_from_context(context)
    rule_id = get_rule_id(context)
    allowed_to_proceed(rule_id)
    rule = get_rule_by_id(rule_id)
    if rule is not None:
        forward = rule['forecast_days']
        backward = rule['observed_days']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        run_hechms_preprocess_cmd = run_hechms_preprocess_cmd_template.format(run_node, run_port, exec_date, backward,
                                                                              forward)
        print('get_run_hechms_preprocess_cmd|run_hechms_preprocess_cmd : ', run_hechms_preprocess_cmd)
        request_url = run_hechms_preprocess_request.format(run_node, run_port, exec_date, backward, forward)
        print('get_run_hechms_preprocess_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_run_hechms_preprocess_cmd|success')
        else:
            raise AirflowException(
                'get_run_hechms_preprocess_cmd|failed'
            )
    else:
        raise AirflowException(
            'get_run_hechms_preprocess_cmd|hechms rule not found'
        )


def get_run_hechms_cmd(**context):
    rule_id = get_rule_id(context)
    allowed_to_proceed(rule_id)
    rule = get_rule_by_id(rule_id)
    if rule is not None:
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        run_hechms_cmd = run_hechms_cmd_template.format(run_node, run_port)
        print('get_run_hechms_preprocess_cmd|run_hechms_cmd : ', run_hechms_cmd)
        request_url = run_hechms_cmd_request.format(run_node, run_port)
        print('get_run_hechms_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_run_hechms_cmd|success')
        else:
            raise AirflowException(
                'get_run_hechms_cmd|failed'
            )
    else:
        raise AirflowException(
            'get_run_hechms_cmd|hechms rule not found'
        )


def get_run_hechms_postprocess_cmd(**context):
    exec_date = get_local_exec_date_from_context(context)
    rule_id = get_rule_id(context)
    allowed_to_proceed(rule_id)
    rule = get_rule_by_id(rule_id)
    if rule is not None:
        forward = rule['forecast_days']
        backward = rule['observed_days']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        run_hechms_postprocess_cmd = run_hechms_postprocess_cmd_template.format(run_node, run_port, exec_date, backward,
                                                                                forward)
        print('get_run_hechms_postprocess_cmd|run_hechms_postprocess_cmd : ', run_hechms_postprocess_cmd)
        request_url = run_hechms_postprocess_request.format(run_node, run_port, exec_date, backward, forward)
        print('get_run_hechms_postprocess_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_run_hechms_postprocess_cmd|success')
        else:
            raise AirflowException(
                'get_run_hechms_postprocess_cmd|failed'
            )
    else:
        raise AirflowException(
            'get_run_hechms_postprocess_cmd|hechms rule not found'
        )


def get_upload_discharge_cmd(**context):
    exec_date = get_local_exec_date_from_context(context)
    rule_id = get_rule_id(context)
    allowed_to_proceed(rule_id)
    rule = get_rule_by_id(rule_id)
    if rule is not None:
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
    else:
        raise AirflowException(
            'get_upload_discharge_cmd|hechms rule not found'
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


def get_dss_db_adapter():
    adapter = None
    try:
        db_config = Variable.get('db_config', deserialize_json=True)
        try:
            adapter = RuleEngineAdapter.get_instance(db_config)
        except Exception as ex:
            print('get_dss_db_adapter|Exception: ', str(ex))
    except Exception as e:
        print('get_dss_db_adapter|db_config|Exception: ', str(e))
    return adapter


def get_rule_by_id(rule_id):
    db_adapter = get_dss_db_adapter()
    if db_adapter:
        wrf_rule = db_adapter.get_hechms_rule_info_by_id(rule_id)
        return wrf_rule
    else:
        print('db adapter error')
        return None


def allowed_to_proceed(rule_id):
    adapter = get_dss_db_adapter()
    if adapter is not None:
        result = adapter.get_hechms_rule_status_by_id(rule_id)
        print('allowed_to_proceed|result : ', result)
        if result is not None:
            if result['status'] == 5:
                raise AirflowException(
                    'Dag has stopped by admin.'
                )
            else:
                print('Allowed to proceed')
        else:
            print('Allowed to proceed')
    else:
        print('Allowed to proceed')


def push_rule_to_xcom(dag_run, **kwargs):
    print('run_this_func|dag_run : ', dag_run)
    hechms_rule = dag_run.conf
    print('run_this_func|hechms_rule : ', hechms_rule)
    return hechms_rule


def get_rule_id(context):
    rule_id = context['task_instance'].xcom_pull(task_ids='init_hechms')['id']
    if rule_id:
        print('get_rule_id|rule_id : ', rule_id)
        return rule_id
    else:
        return None


def on_dag_failure(context):
    rule_id = get_rule_id(context)
    if rule_id is not None:
        update_workflow_status(4, rule_id)
        print('on_dag_failure|set error status for rule|rule_id :', rule_id)
    else:
        print('on_dag_failure|rule_id not found')


def create_dag(dag_id, dag_rule, timeout, default_args):
    print('create_dag|dag_rule : ', dag_rule)
    dag = DAG(dag_id, catchup=False,
              dagrun_timeout=timeout,
              schedule_interval=None,
              params=dag_rule,
              on_failure_callback=on_dag_failure,
              is_paused_upon_creation=False,
              default_args=default_args)

    with dag:
        init_hechms = PythonOperator(
            task_id='init_hechms',
            provide_context=True,
            python_callable=push_rule_to_xcom,
            params=dag_rule,
            pool=dag_pool
        )

        running_status = PythonOperator(
            task_id='set_running_status',
            provide_context=True,
            python_callable=set_running_status,
            params=dag_rule,
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

        complete_state = PythonOperator(
            task_id='complete_state',
            provide_context=True,
            params=dag_rule,
            python_callable=set_complete_status,
            pool=dag_pool
        )

        init_hechms >> running_status >> create_input_hec_dis >> run_hechms_preprocess_hec_dis >> \
        run_hechms_hec_dis >> run_hechms_postprocess_hec_dis >> upload_discharge_hec_dis >> complete_state

    return dag


def get_timeout(timeout):
    print('get_timeout|timeout : ', timeout)
    timeout_in_timedelta = timedelta(hours=timeout['hours'], minutes=timeout['minutes'], seconds=timeout['seconds'])
    print('get_timeout|timeout_in_timedelta : ', timeout_in_timedelta)
    return timeout_in_timedelta


def generate_hechms_workflow_dag(dag_rule):
    print('generate_hechms_workflow_dag|dag_rule : ', dag_rule)
    if dag_rule:
        timeout = get_timeout(dag_rule['timeout'])
        default_args = {
            'owner': 'dss admin',
            'start_date': datetime.utcnow(),
            'email': ['hasithadkr7@gmail.com'],
            'email_on_failure': True,
            'retries': 1,
            'retry_delay': timedelta(seconds=30)
        }
        dag_id = dag_rule['name']
        globals()[dag_id] = create_dag(dag_id, dag_rule, timeout, default_args)


def create_hechms_dags():
    db_config = Variable.get('db_config', deserialize_json=True)
    print('start_creating|db_config : ', db_config)
    adapter = RuleEngineAdapter.get_instance(db_config)
    rules = adapter.get_all_hechms_rules()
    print('create_hechms_dags|rules : ', rules)
    if len(rules) > 0:
        for rule in rules:
            try:
                generate_hechms_workflow_dag(rule)
            except Exception as e:
                print('generate_hechms_workflow_dag|Exception: ', str(e))


create_hechms_dags()