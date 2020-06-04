from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import sys
import requests

sys.path.insert(0, '/home/uwcc-admin/new_dss/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

prod_dag_name = 'flo2d_10_Prwarama_Rd_dag'
dag_pool = 'flo2d_10m_pool'

MAIN_SERVER_IP = '10.138.0.18'
MAIN_SERVER_PORT = 8080


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7gmail.com'],
    'email_on_failure': True,
}


start_server_cmd_request = 'http://{}:{}/start-flo2d-server?' \
                          'host={}&port={}'

create_rain_cmd_request = 'http://{}:{}/create-rain?' \
                              'run_date={}&run_time={}&model={}' \
                              '&forward={}&backward={}&pop_method={}'

run_flo2d_cmd_request = 'http://{}:{}/run-flo2d?' \
                        'run_date={}&run_time={}&model={}' \
                        '&forward={}&backward={}'

create_ascii_cmd_request = 'http://{}:{}/create-ascii?' \
                           'run_date={}&run_time={}&model={}' \
                           '&forward={}&backward={}'

create_max_wl_map_cmd_request = 'http://{}:{}/create-max-wl-map?' \
                                'run_date={}&run_time={}&model={}' \
                                '&forward={}&backward={}'

shutdown_server_cmd_request = 'http://{}:{}/shutdown-server'


def send_http_get_request(url, params=None):
    if params is not None:
        r = requests.get(url=url, params=params)
    else:
        r = requests.get(url=url)
    response = r.json()
    print('send_http_get_request|response : ', response)
    if response == {'response': 'success'}:
        return True
    return False


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
    rule_info = context['task_instance'].xcom_pull(task_ids='init_flo2d_10m')['rule_info']
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


def get_local_exec_date_time_from_context(context):
    rule = context['task_instance'].xcom_pull(task_ids='init_flo2d_10m')
    if 'run_date' in rule:
        exec_datetime_str = rule['run_date']
        exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S')
        exec_date = exec_datetime.strftime('%Y-%m-%d')
        exec_time = exec_datetime.strftime('%H:00:00')
    else:
        exec_datetime_str = context["execution_date"].to_datetime_string()
        exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S') \
                        - timedelta(days=1) + timedelta(hours=5, minutes=30)
        exec_date = exec_datetime.strftime('%Y-%m-%d')
        exec_time = exec_datetime.strftime('%H:00:00')
    return [exec_date, exec_time]


def get_dss_adapter():
    adapter = None
    try:
        db_config = Variable.get('db_config', deserialize_json=True)
        try:
            adapter = RuleEngineAdapter.get_instance(db_config)
        except Exception as ex:
            print('update_workflow_status|db_adapter|Exception: ', str(ex))
    except Exception as e:
        print('update_workflow_status|db_config|Exception: ', str(e))
    finally:
        return adapter


def is_allowed_to_run(rule_id):
    adapter = get_dss_adapter()
    if adapter is not None:
        result = adapter.get_flo2d_rule_status_by_id(rule_id)
        if result is not None:
            if result['status'] == 5:
                return False
            else:
                return True
        else:
            return True
    else:
        return True


def get_create_rain_cmd(**context):
    rule_id = get_rule_id(context)
    rule = context['task_instance'].xcom_pull(task_ids='init_flo2d')
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    if is_allowed_to_run(rule_id):
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        pop_method = rule['raincell_data_from']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        request_url = create_rain_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                         target_model, forward, backward, pop_method)
        print('get_create_rain_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_create_rain_cmd|success')
        else:
            raise AirflowException(
                'get_create_rain_cmd|failed'
            )
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


def get_run_flo2d_cmd(**context):
    rule_id = get_rule_id(context)
    rule = context['task_instance'].xcom_pull(task_ids='init_flo2d')
    if is_allowed_to_run(rule_id):
        [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        request_url = run_flo2d_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                   target_model, forward, backward)
        print('get_run_flo2d_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_run_flo2d_cmd|success')
        else:
            raise AirflowException(
                'get_run_flo2d_cmd|failed'
            )
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


def create_multi_ascii(**context):
    rule_id = get_rule_id(context)
    rule = context['task_instance'].xcom_pull(task_ids='init_flo2d')
    if is_allowed_to_run(rule_id):
        [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        request_url = create_ascii_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                      target_model, forward, backward)
        print('create_multi_ascii|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('create_multi_ascii|success')
        else:
            raise AirflowException(
                'create_multi_ascii|failed'
            )
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


def create_max_wl_map(**context):
    rule_id = get_rule_id(context)
    rule = context['task_instance'].xcom_pull(task_ids='init_flo2d')
    if is_allowed_to_run(rule_id):
        [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        request_url = create_max_wl_map_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                           target_model, forward, backward)
        print('create_max_wl_map|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('create_max_wl_map|success')
        else:
            raise AirflowException(
                'create_max_wl_map|failed'
            )
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


def run_this_func(dag_run, **kwargs):
    print('run_this_func|dag_run : ', dag_run)
    flo2d_rule = {'model': '10m', 'rule_info': dag_run.conf}
    print('run_this_func|flo2d_rule : ', flo2d_rule)
    return flo2d_rule


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run Flo2d 10m DAG', catchup=False) as dag:
    init_flo2d = PythonOperator(
        task_id='init_flo2d',
        provide_context=True,
        python_callable=run_this_func,
        pool=dag_pool
    )

    running_state_flo2d = PythonOperator(
        task_id='running_state_flo2d',
        provide_context=True,
        python_callable=set_running_status,
        dag=dag,
        pool=dag_pool
    )

    create_rain_flo2d = PythonOperator(
        task_id='create_rain_flo2d',
        provide_context=True,
        python_callable=get_create_rain_cmd,
        pool=dag_pool
    )

    run_flo2d = PythonOperator(
        task_id='run_flo2d',
        provide_context=True,
        python_callable=get_run_flo2d_cmd,
        pool=dag_pool
    )

    create_multi_ascii_set = PythonOperator(
        task_id='create_multi_ascii_set',
        provide_context=True,
        python_callable=create_multi_ascii,
        pool=dag_pool
    )

    create_max_wl_ascii_map = PythonOperator(
        task_id='create_max_wl_ascii_map',
        provide_context=True,
        python_callable=create_max_wl_map,
        pool=dag_pool
    )

    complete_state_flo2d_10m = PythonOperator(
        task_id='complete_state_flo2d_10m',
        provide_context=True,
        python_callable=set_complete_status,
        dag=dag,
        pool=dag_pool
    )

    init_flo2d >> running_state_flo2d >> create_rain_flo2d >> \
    run_flo2d >> create_multi_ascii_set >> create_max_wl_ascii_map >> \
    complete_state_flo2d_10m

