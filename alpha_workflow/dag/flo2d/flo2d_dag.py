from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import sys
import requests

# from requests import session, Timeout

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/utils')
from db_util import RuleEngineAdapter

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

run_flo2d_cmd_template = 'curl -X GET "http://{}:{}/run-flo2d?' \
                         'run_date={}&run_time={}&model={}' \
                         '&forward={}&backward={}"'
run_flo2d_cmd_request = 'http://{}:{}/run-flo2d?' \
                        'run_date={}&run_time={}&model={}' \
                        '&forward={}&backward={}'

extract_water_level_cmd_template = 'curl -X GET "http://{}:{}/extract-water-level?' \
                                   'run_date={}&run_time={}&model={}' \
                                   '&forward={}&backward={}&sim_tag={}"'
extract_water_level_cmd_request = 'http://{}:{}/extract-water-level?' \
                                  'run_date={}&run_time={}&model={}' \
                                  '&forward={}&backward={}&sim_tag={}'

extract_water_discharge_cmd_template = 'curl -X GET "http://{}:{}/extract-discharge?' \
                                       'run_date={}&run_time={}&model={}' \
                                       '&forward={}&backward={}&sim_tag={}"'
extract_water_discharge_cmd_request = 'http://{}:{}/extract-discharge?' \
                                      'run_date={}&run_time={}&model={}' \
                                      '&forward={}&backward={}&sim_tag={}'

create_ascii_cmd_request = 'http://{}:{}/create-ascii?' \
                        'run_date={}&run_time={}&model={}' \
                        '&forward={}&backward={}'

create_max_wl_map_cmd_request = 'http://{}:{}/create-max-wl-map?' \
                        'run_date={}&run_time={}&model={}' \
                        '&forward={}&backward={}'


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


def get_local_exec_date_time_from_context(context):
    rule = context['task_instance'].xcom_pull(task_ids='init_flo2d')
    if 'run_date' in rule:
        exec_datetime_str = rule['run_date']
        exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S')
        exec_date = exec_datetime.strftime('%Y-%m-%d')
        exec_time = exec_datetime.strftime('%H:%M:%S')
    else:
        exec_datetime_str = context["execution_date"].to_datetime_string()
        exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S') \
                        - timedelta(days=1) + timedelta(hours=5, minutes=30)
        exec_date = exec_datetime.strftime('%Y-%m-%d')
        exec_time = exec_datetime.strftime('%H:00:00')
    return [exec_date, exec_time]


def get_create_raincell_cmd(**context):
    rule_id = get_rule_id(context)
    rule = get_rule_by_id(rule_id)
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    if is_allowed_to_run(rule_id):
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        pop_method = rule['raincell_data_from']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        create_raincell_cmd = create_raincell_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                                  target_model, forward, backward, pop_method)
        print('get_create_raincell_cmd|create_raincell_cmd : ', create_raincell_cmd)
        request_url = create_raincell_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                         target_model, forward, backward, pop_method)
        print('get_create_raincell_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_create_raincell_cmd|success')
        else:
            raise AirflowException(
                'get_create_raincell_cmd|failed'
            )
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


def get_create_inflow_cmd(**context):
    rule_id = get_rule_id(context)
    rule = get_rule_by_id(rule_id)
    if is_allowed_to_run(rule_id):
        [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        pop_method = rule['inflow_data_from']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
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
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


def get_create_chan_cmd(**context):
    rule_id = get_rule_id(context)
    rule = get_rule_by_id(rule_id)
    if is_allowed_to_run(rule_id):
        [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        create_chan_cmd = create_chan_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                          target_model, forward, backward)
        print('get_create_chan_cmd|create_chan_cmd : ', create_chan_cmd)
        request_url = create_chan_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                     target_model, forward, backward)
        print('get_create_chan_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_create_chan_cmd|success')
        else:
            raise AirflowException(
                'get_create_chan_cmd|failed'
            )
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


def get_create_outflow_cmd(**context):
    rule_id = get_rule_id(context)
    rule = get_rule_by_id(rule_id)
    if is_allowed_to_run(rule_id):
        [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        pop_method = rule['outflow_data_from']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        create_outflow_cmd = create_outflow_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                                target_model, forward, backward, pop_method)
        print('get_create_outflow_cmd|create_outflow_cmd : ', create_outflow_cmd)
        request_url = create_outflow_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                        target_model, forward, backward, pop_method)
        print('get_create_outflow_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_create_outflow_cmd|success')
        else:
            raise AirflowException(
                'get_create_outflow_cmd|failed'
            )
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


def get_run_flo2d_cmd(**context):
    rule_id = get_rule_id(context)
    rule = get_rule_by_id(rule_id)
    if is_allowed_to_run(rule_id):
        [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        run_flo2d_cmd = run_flo2d_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                      target_model, forward, backward)
        print('get_run_flo2d_cmd|run_flo2d_cmd : ', run_flo2d_cmd)
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


def get_extract_water_level_cmd(**context):
    rule_id = get_rule_id(context)
    rule = get_rule_by_id(rule_id)
    if is_allowed_to_run(rule_id):
        [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        sim_tag = 'event_run'
        extract_water_level_cmd = extract_water_level_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                                          target_model, forward, backward, sim_tag)
        print('get_extract_water_level_cmd|extract_water_level_cmd : ', extract_water_level_cmd)
        # subprocess.call(extract_water_level_cmd, shell=True)
        request_url = extract_water_level_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                             target_model, forward, backward, sim_tag)
        print('get_extract_water_level_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_extract_water_level_cmd|success')
        else:
            raise AirflowException(
                'get_extract_water_level_cmd|failed'
            )
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


def get_extract_water_discharge_cmd(**context):
    rule_id = get_rule_id(context)
    rule = get_rule_by_id(rule_id)
    if is_allowed_to_run(rule_id):
        [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        sim_tag = 'event_run'
        extract_water_discharge_cmd = extract_water_discharge_cmd_template.format(run_node, run_port, exec_date,
                                                                                  exec_time,
                                                                                  target_model, forward, backward,
                                                                                  sim_tag)
        print('get_extract_water_discharge_cmd|extract_water_discharge_cmd : ', extract_water_discharge_cmd)
        request_url = extract_water_discharge_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                                 target_model, forward, backward, sim_tag)
        print('get_extract_water_discharge_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_extract_water_discharge_cmd|success')
        else:
            raise AirflowException(
                'get_extract_water_discharge_cmd|failed'
            )
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


def get_extract_water_level_cmd(**context):
    rule_id = get_rule_id(context)
    rule = get_rule_by_id(rule_id)
    if is_allowed_to_run(rule_id):
        [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
        forward = rule['forecast_days']
        backward = rule['observed_days']
        target_model = rule['target_model']
        run_node = rule['rule_details']['run_node']
        run_port = rule['rule_details']['run_port']
        sim_tag = 'event_run'
        extract_water_level_cmd = extract_water_level_cmd_template.format(run_node, run_port, exec_date, exec_time,
                                                                          target_model, forward, backward, sim_tag)
        print('get_extract_water_level_cmd|extract_water_level_cmd : ', extract_water_level_cmd)
        request_url = extract_water_level_cmd_request.format(run_node, run_port, exec_date, exec_time,
                                                             target_model, forward, backward, sim_tag)
        print('get_extract_water_level_cmd|request_url : ', request_url)
        if send_http_get_request(request_url):
            print('get_extract_water_level_cmd|success')
        else:
            raise AirflowException(
                'get_extract_water_level_cmd|failed'
            )
    else:
        raise AirflowException(
            'Dag has stopped by admin.'
        )


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


def get_rule_by_id(rule_id):
    db_adapter = get_dss_db_adapter()
    if db_adapter:
        wrf_rule = db_adapter.get_flo2d_rule_info_by_id(rule_id)
        return wrf_rule
    else:
        print('db adapter error')
        return None


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


def get_rule_id(context):
    rule_info = context['task_instance'].xcom_pull(task_ids='init_flo2d')
    if rule_info:
        rule_id = rule_info['id']
        print('get_rule_id|rule_id : ', rule_id)
        return rule_id
    else:
        return None


def get_run_type(context):
    rule_info = context['task_instance'].xcom_pull(task_ids='init_flo2d')
    if rule_info:
        run_type = rule_info['run_type']
        print('get_run_type|run_type : ', run_type)
        return run_type
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
    flo2d_rule = dag_run.conf
    print('run_this_func|flo2d_rule : ', flo2d_rule)
    return flo2d_rule


def on_dag_failure(context):
    rule_id = get_rule_id(context)
    if rule_id is not None:
        update_workflow_status(4, rule_id)
        print('on_dag_failure|set error status for rule|rule_id :', rule_id)
    else:
        print('on_dag_failure|rule_id not found')


def update_max_water_levels(**context):
    rule_id = get_rule_id(context)
    rule = get_rule_by_id(rule_id)
    print('update_max_water_levels|rule : ', rule)
    dss_adapter = get_dss_db_adapter()
    fcst_db_config = Variable.get('fcst_db_config', deserialize_json=True)
    run_type = get_run_type(context)
    print('update_max_water_levels|run_type : ', run_type)
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    exec_datetime = '{} {}'.format(exec_date, exec_time)
    print('update_max_water_levels|exec_datetime : ', exec_datetime)
    if run_type is not None:
        if run_type == 'event':
            sim_tag = 'event_run'
        else:
            sim_tag = 'event_run'
        if rule['target_model'] == 'flo2d_250':
            locations = dss_adapter.get_water_level_locations()
            print('update_max_water_levels|locations : ', locations)
            dss_adapter.update_max_flo2d_250_forecast_water_level(locations, exec_datetime, sim_tag, fcst_db_config)
        else:
            print('update_max_water_levels|ToDo..')


def create_multi_ascii(**context):
    rule_id = get_rule_id(context)
    rule = get_rule_by_id(rule_id)
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
    rule = get_rule_by_id(rule_id)
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
            pool=dag_pool
        )

        create_raincell_flo2d = PythonOperator(
            task_id='create_raincell_flo2d',
            provide_context=True,
            python_callable=get_create_raincell_cmd,
            pool=dag_pool
        )

        create_inflow_flo2d = PythonOperator(
            task_id='create_inflow_flo2d',
            provide_context=True,
            python_callable=get_create_inflow_cmd,
            pool=dag_pool
        )

        create_chan_flo2d = PythonOperator(
            task_id='create_chan_flo2d',
            provide_context=True,
            python_callable=get_create_chan_cmd,
            pool=dag_pool
        )

        create_outflow_flo2d = PythonOperator(
            task_id='create_outflow_flo2d',
            provide_context=True,
            python_callable=get_create_outflow_cmd,
            pool=dag_pool
        )

        run_flo2d_flo2d = PythonOperator(
            task_id='run_flo2d_flo2d',
            provide_context=True,
            python_callable=get_run_flo2d_cmd,
            trigger_rule='none_failed',
            pool=dag_pool
        )

        extract_water_level_flo2d = PythonOperator(
            task_id='extract_water_level_flo2d',
            provide_context=True,
            python_callable=get_extract_water_level_cmd,
            pool=dag_pool
        )

        extract_water_discharge_flo2d = PythonOperator(
            task_id='extract_water_discharge_flo2d',
            provide_context=True,
            python_callable=get_extract_water_discharge_cmd,
            pool=dag_pool
        )

        update_max_water_level = PythonOperator(
            task_id='update_max_water_level',
            provide_context=True,
            python_callable=update_max_water_levels,
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

        complete_state = PythonOperator(
            task_id='complete_state',
            provide_context=True,
            python_callable=set_complete_status,
            pool=dag_pool
        )

        init_flo2d >> running_state_flo2d >> create_raincell_flo2d >> create_chan_flo2d >> \
        create_inflow_flo2d >> create_outflow_flo2d >> run_flo2d_flo2d >> \
        extract_water_level_flo2d >> extract_water_discharge_flo2d >> update_max_water_level >> \
        create_multi_ascii_set >> create_max_wl_ascii_map >> complete_state

    return dag


def get_timeout(timeout):
    print('get_timeout|timeout : ', timeout)
    timeout_in_timedelta = timedelta(hours=timeout['hours'], minutes=timeout['minutes'], seconds=timeout['seconds'])
    print('get_timeout|timeout_in_timedelta : ', timeout_in_timedelta)
    return timeout_in_timedelta


def generate_flo2d_workflow_dag(dag_rule):
    print('generate_flo2d_workflow_dag|dag_rule : ', dag_rule)
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


def create_flo2d_dags():
    db_config = Variable.get('db_config', deserialize_json=True)
    print('start_creating|db_config : ', db_config)
    adapter = RuleEngineAdapter.get_instance(db_config)
    rules = adapter.get_all_flo2d_rules()
    print('create_flo2d_dags|rules : ', rules)
    if len(rules) > 0:
        for rule in rules:
            try:
                generate_flo2d_workflow_dag(rule)
            except Exception as e:
                print('generate_flo2d_workflow_dag|Exception: ', str(e))


create_flo2d_dags()
