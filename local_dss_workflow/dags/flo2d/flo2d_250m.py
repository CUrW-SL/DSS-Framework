from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
import sys
import subprocess
import requests
# from requests import session, Timeout

sys.path.insert(0, '/home/curw/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

prod_dag_name = 'flo2d_250m_dag'
dag_pool = 'flo2d_pool'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7gmail.com'],
    'email_on_failure': True,
}

create_raincell_cmd_template = 'curl -X GET "http://{}:{}/create-raincell?' \
                               'run_date={}&run_time={}&model={}' \
                               '&forward={}&backward={}&pop_method={}"'
create_raincell_cmd_request = 'http://{}:{}/create-raincell?' \
                               'run_date={}&run_time={}&model={}' \
                               '&forward={}&backward={}&pop_method={}'

create_inflow_cmd_template = 'curl -X GET "http://{}:{}/create-inflow?' \
                             'run_date={}&run_time={}&model={}' \
                             '&forward={}&backward={}&pop_method={}"'
create_inflow_cmd_request = 'http://{}:{}/create-inflow?' \
                             'run_date={}&run_time={}&model={}' \
                             '&forward={}&backward={}&pop_method={}'

create_outflow_cmd_template = 'curl -X GET "http://{}:{}/create-outflow?' \
                              'run_date={}&run_time={}&model={}' \
                              '&forward={}&backward={}&pop_method={}"'
create_outflow_cmd_request = 'http://{}:{}/create-outflow?' \
                              'run_date={}&run_time={}&model={}' \
                              '&forward={}&backward={}&pop_method={}'

create_chan_cmd_template = 'curl -X GET "http://{}:{}/create-chan?' \
                           'run_date={}&run_time={}&model={}' \
                           '&forward={}&backward={}"'
create_chan_cmd_request = 'http://{}:{}/create-chan?' \
                           'run_date={}&run_time={}&model={}' \
                           '&forward={}&backward={}'

run_flo2d_250m_cmd_template = 'curl -X GET "http://{}:{}/run-flo2d?' \
                              'run_date={}&run_time={}&model={}' \
                              '&forward={}&backward={}"'
run_flo2d_250m_cmd_request = 'http://{}:{}/run-flo2d?' \
                              'run_date={}&run_time={}&model={}' \
                              '&forward={}&backward={}'

extract_water_level_cmd_template = 'curl -X GET "http://{}:{}/extract-water-level?' \
                                   'run_date={}&run_time={}&model={}' \
                                   '&forward={}&backward={}&sim_tag={}"'
extract_water_level_cmd_request = 'http://{}:{}/extract-data?' \
                                   'run_date={}&run_time={}&model={}' \
                                   '&forward={}&backward={}&sim_tag={}'

extract_water_discharge_cmd_template = 'curl -X GET "http://{}:{}/extract-discharge?' \
                                   'run_date={}&run_time={}&model={}' \
                                   '&forward={}&backward={}&sim_tag={}"'
extract_water_discharge_cmd_request = 'http://{}:{}/extract-discharge?' \
                                  'run_date={}&run_time={}&model={}' \
                                  '&forward={}&backward={}&sim_tag={}'


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


def get_rule_from_context(context):
    rule = context['task_instance'].xcom_pull(task_ids='init_flo2d_250m')
    print('get_rule_from_context|rule : ', rule)
    return rule


def get_local_exec_date_time_from_context(context):
    rule = get_rule_from_context(context)
    if 'run_date' in rule['rule_info']:
        exec_datetime_str = rule['rule_info']['run_date']
        exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S')
        exec_date = exec_datetime.strftime('%Y-%m-%d')
        exec_time = exec_datetime.strftime('%H:%M:%S')
    else:
        exec_datetime_str = context["execution_date"].to_datetime_string()
        exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S') \
                        - timedelta(days=1) + timedelta(hours=5, minutes=30)
        exec_date = exec_datetime.strftime('%Y-%m-%d')
        # exec_time = exec_datetime.strftime('%H:%M:%S')
        exec_time = exec_datetime.strftime('%H:00:00')
    return [exec_date, exec_time]


def get_create_raincell_cmd(**context):
    rule = get_rule_from_context(context)
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    target_model = rule['rule_info']['target_model']
    pop_method = rule['rule_info']['raincell_data_from']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    create_raincell_cmd = create_raincell_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                              target_model, forward, backward, pop_method)
    print('get_create_raincell_cmd|create_raincell_cmd : ', create_raincell_cmd)
    #subprocess.call(create_raincell_cmd, shell=True)
    request_url = create_raincell_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                     target_model, forward, backward, pop_method)
    print('get_create_raincell_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_create_raincell_cmd|success')
    else:
        raise AirflowException(
            'get_create_raincell_cmd|failed'
        )


def get_create_inflow_cmd(**context):
    rule = get_rule_from_context(context)
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    target_model = rule['rule_info']['target_model']
    pop_method = rule['rule_info']['raincell_data_from']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    create_inflow_cmd = create_inflow_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                          target_model, forward, backward, pop_method)
    print('get_create_inflow_cmd|create_inflow_cmd : ', create_inflow_cmd)
    # subprocess.call(create_inflow_cmd, shell=True)
    request_url = create_inflow_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                   target_model, forward, backward, pop_method)
    print('get_create_inflow_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_create_inflow_cmd|success')
    else:
        raise AirflowException(
            'get_create_inflow_cmd|failed'
        )


def get_create_chan_cmd(**context):
    rule = get_rule_from_context(context)
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    target_model = rule['rule_info']['target_model']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    create_chan_cmd = create_chan_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                      target_model, forward, backward)
    print('get_create_chan_cmd|create_chan_cmd : ', create_chan_cmd)
    #subprocess.call(create_chan_cmd, shell=True)
    request_url = create_chan_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                 target_model, forward, backward)
    print('get_create_chan_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_create_chan_cmd|success')
    else:
        raise AirflowException(
            'get_create_chan_cmd|failed'
        )


def get_create_outflow_cmd(**context):
    rule = get_rule_from_context(context)
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    target_model = rule['rule_info']['target_model']
    pop_method = rule['rule_info']['raincell_data_from']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    create_outflow_cmd = create_outflow_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                            target_model, forward, backward, pop_method)
    print('get_create_outflow_cmd|create_outflow_cmd : ', create_outflow_cmd)
    #subprocess.call(create_outflow_cmd, shell=True)
    request_url = create_outflow_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                    target_model, forward, backward, pop_method)
    print('get_create_outflow_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_create_outflow_cmd|success')
    else:
        raise AirflowException(
            'get_create_outflow_cmd|failed'
        )


def get_run_flo2d_250m_cmd(**context):
    rule = get_rule_from_context(context)
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    target_model = rule['rule_info']['target_model']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    run_flo2d_250m_cmd = run_flo2d_250m_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                            target_model, forward, backward)
    print('get_run_flo2d_250m_cmd|run_flo2d_250m_cmd : ', run_flo2d_250m_cmd)
    #subprocess.call(run_flo2d_250m_cmd, shell=True)
    request_url = run_flo2d_250m_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                    target_model, forward, backward)
    print('get_run_flo2d_250m_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_run_flo2d_250m_cmd|success')
    else:
        raise AirflowException(
            'get_run_flo2d_250m_cmd|failed'
        )


def get_extract_water_level_cmd(**context):
    rule = get_rule_from_context(context)
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    target_model = rule['rule_info']['target_model']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    sim_tag = 'event_run'
    extract_water_level_cmd = extract_water_level_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                                      target_model, forward, backward, sim_tag)
    print('get_extract_water_level_cmd|extract_water_level_cmd : ', extract_water_level_cmd)
    subprocess.call(extract_water_level_cmd, shell=True)
    # request_url = extract_water_level_cmd_request.format(run_node, run_port, exec_date, exec_time,
    #                                                      target_model, forward, backward, sim_tag)
    # print('get_extract_water_level_cmd|request_url : ', request_url)
    # if send_http_get_request(request_url):
    #     print('get_extract_water_level_cmd|success')
    # else:
    #     raise AirflowException(
    #         'get_extract_water_level_cmd|failed'
    #     )


def get_extract_water_discharge_cmd(**context):
    rule = get_rule_from_context(context)
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    forward = rule['rule_info']['forecast_days']
    backward = rule['rule_info']['observed_days']
    target_model = rule['rule_info']['target_model']
    run_node = rule['rule_info']['rule_details']['run_node']
    run_port = rule['rule_info']['rule_details']['run_port']
    sim_tag = 'event_run'
    extract_water_discharge_cmd = extract_water_discharge_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                                      target_model, forward, backward, sim_tag)
    print('get_extract_water_discharge_cmd|extract_water_discharge_cmd : ', extract_water_discharge_cmd)
    subprocess.call(extract_water_discharge_cmd, shell=True)
    # request_url = extract_water_discharge_cmd_request.format(run_node, run_port, exec_date, exec_time,
    #                                                      target_model, forward, backward, sim_tag)
    # print('get_extract_water_discharge_cmd|request_url : ', request_url)
    # if send_http_get_request(request_url):
    #     print('get_extract_water_discharge_cmd|success')
    # else:
    #     raise AirflowException(
    #         'get_extract_water_discharge_cmd|failed'
    #     )


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


def on_dag_failure(context):
    rule_id = get_rule_id(context)
    if rule_id is not None:
        update_workflow_status(4, rule_id)
        print('on_dag_failure|set error status for rule|rule_id :', rule_id)
    else:
        print('on_dag_failure|rule_id not found')


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run Flo2d 250m DAG', dagrun_timeout=timedelta(minutes=55), catchup=False,
         on_failure_callback=on_dag_failure) as dag:
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
        execution_timeout=timedelta(minutes=45),
        python_callable=get_create_raincell_cmd,
        pool=dag_pool
    )

    create_inflow_flo2d_250m = PythonOperator(
        task_id='create_inflow_flo2d_250m',
        provide_context=True,
        execution_timeout=timedelta(minutes=5),
        python_callable=get_create_inflow_cmd,
        pool=dag_pool
    )

    create_chan_flo2d_250m = PythonOperator(
        task_id='create_chan_flo2d_250m',
        provide_context=True,
        execution_timeout=timedelta(minutes=5),
        python_callable=get_create_chan_cmd,
        pool=dag_pool
    )

    create_outflow_flo2d_250m = PythonOperator(
        task_id='create_outflow_flo2d_250m',
        provide_context=True,
        execution_timeout=timedelta(minutes=5),
        python_callable=get_create_outflow_cmd,
        pool=dag_pool
    )

    run_flo2d_250m_flo2d_250m = PythonOperator(
        task_id='run_flo2d_250m_flo2d_250m',
        provide_context=True,
        python_callable=get_run_flo2d_250m_cmd,
        execution_timeout=timedelta(minutes=45),
        trigger_rule='none_failed',
        pool=dag_pool
    )

    extract_water_level_flo2d_250m = PythonOperator(
        task_id='extract_water_level_flo2d_250m',
        provide_context=True,
        python_callable=get_extract_water_level_cmd,
        pool=dag_pool
    )

    extract_water_discharge_flo2d_250m = PythonOperator(
        task_id='extract_water_discharge_flo2d_250m',
        provide_context=True,
        python_callable=get_extract_water_discharge_cmd,
        pool=dag_pool
    )

    complete_state_flo2d_250m = PythonOperator(
        task_id='complete_state',
        provide_context=True,
        python_callable=set_complete_status,
        pool=dag_pool
    )

    init_flo2d_250m >> running_state_flo2d_250m >> create_raincell_flo2d_250m >> create_chan_flo2d_250m >> \
    create_inflow_flo2d_250m >>  create_outflow_flo2d_250m >> run_flo2d_250m_flo2d_250m >> \
    extract_water_level_flo2d_250m >> extract_water_discharge_flo2d_250m >> complete_state_flo2d_250m
    # check_accuracy_flo2d250m >> complete_state_flo2d_250m
    # extract_water_level_flo2d_250m >> check_accuracy_flo2d250m >> complete_state_flo2d_250m

