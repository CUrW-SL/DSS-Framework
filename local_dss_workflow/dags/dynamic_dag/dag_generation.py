from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.sql_sensor import SqlSensor
import sys

sys.path.insert(0, '/home/curw/git/DSS-Framework/gen_util')
from dynamic_dag_util import get_all_dynamic_dag_routines, get_dynamic_dag_tasks, get_trigger_target_dag

sys.path.insert(0, '/home/curw/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/curw/git/DSS-Framework/local_dss_workflow/plugins/operators')
from dynamic_external_trigger_operator import DynamicTriggerDagRunOperator


dag_pool = 'external_dag_pool'


def update_workflow_status(status, rule_id):
    adapter = get_dss_db_adapter()
    if adapter is not None:
        adapter.update_dynamic_dag_routing_status(status, rule_id)
    else:
        print('update_workflow_status|db adapter not found.')


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


def set_running_status(**context):
    print('set_running_status|context : ', context)
    params = context['params']
    print('set_running_status|params : ', params)
    routine_id = params['id']
    print('set_running_status|routine_id :', routine_id)
    if routine_id is not None:
        update_workflow_status(2, routine_id)
    else:
        print('set_running_status|rule_id not found')


def set_complete_status(**context):
    print('set_complete_status|context : ', context)
    params = context['params']
    print('set_running_status|params : ', params)
    routine_id = params['id']
    print('set_complete_status|routine_id :', routine_id)
    if routine_id is not None:
        update_workflow_status(3, routine_id)
    else:
        print('set_complete_status|rule_id not found')


def on_dag_failure(context):
    params = context['params']
    print('set_running_status|params : ', params)
    rule_id = params['id']
    if rule_id is not None:
        update_workflow_status(4, rule_id)
        print('on_dag_failure|set error status for rule|rule_id :', rule_id)
    else:
        print('on_dag_failure|rule_id not found')


def create_trigger_dag_run(context):
    print('create_trigger_dag_run|context : ', context)
    run_date = context["execution_date"].to_datetime_string()
    task_name = context['task'].task_id
    dag_rule_id = context['params']['id']
    print('create_trigger_dag_run|[run_date, task_name, dag_rule_id] : ', [run_date, task_name, dag_rule_id])
    dss_adapter = get_dss_db_adapter()
    payload = {}
    dag_info = []
    if dss_adapter is not None:
        target_dag_info = get_trigger_target_dag(dss_adapter, dag_rule_id, task_name)
        print('create_trigger_dag_run|target_dag_info : ', target_dag_info)
        model_type = target_dag_info['input_params']['model_type']
        model_rule = target_dag_info['input_params']['rule_id']
        print('create_trigger_dag_run|[model_type, model_rule] : ', [model_type, model_rule])
        if 'run_date' in target_dag_info['input_params']:
            run_date = target_dag_info['input_params']['run_date']
            print('********create_trigger_dag_run|run on user defined date|run_date : ', run_date)
        print('create_trigger_dag_run|[model_type, model_rule] : ', [model_type, model_rule])
        if model_type == 'wrf':
            payload = dss_adapter.get_eligible_wrf_rule_info_by_id(model_rule)
        elif model_type == 'hechms':
            payload = dss_adapter.get_eligible_hechms_rule_info_by_id(model_rule)
        elif model_type == 'flo2d':
            payload = dss_adapter.get_eligible_flo2d_rule_info_by_id(model_rule)
        elif model_type == 'decision':
            print('create_decision_dag|model_rule : ', model_rule)
            payload = dss_adapter.get_eligible_decision_rule_definition_by_id(model_rule)
        else:
            print('create_trigger_dag_run|available for weather model dags only.')
        if payload is not None:
            payload['run_date'] = run_date
            print('create_trigger_dag_run|payload : ', payload)
            dag_info.append({'dag_name': target_dag_info['task_content'], 'payload': payload})
            print('create_dag_run|dag_info : ', dag_info)
    return dag_info


# example bash command : /home/uwcc-admin/calculate.sh -a 23 -date '2020-01-11' -c 1.4
# sshpass -p 'cfcwm07' ssh curw@124.43.13.195 -p 6022 /home/curw/task.sh -a 12 -b 2 -c 'hello'
def get_bash_command(bash_script, input_params, dag):
    print('get_bash_command|dag : ', dag)
    last_dag_run = dag.get_last_dagrun(include_externally_triggered=True)
    exec_date = last_dag_run.execution_date.strftime('%Y-%m-%d %H:%M:%S')
    print('get_bash_command|exec_date : ', exec_date)
    print('get_bash_command|bash_script : ', bash_script)
    if input_params is None:
        input_params = {}
    if 'run_date' not in input_params:
        # if user hasn't pass 'd' execution date as a param, system will add
        # it to the input params.
        input_params['run_date'] = exec_date
    print('get_bash_command|input_params : ', input_params)
    inputs = []
    if input_params:
        for key in input_params.keys():
            inputs.append('-{} {}'.format(key, input_params[key]))
        if len(inputs) > 0:
            input_str = ' '.join(inputs)
            return '{} {} '.format(bash_script, input_str)
        else:
            return '{} '.format(bash_script)
    else:
        return '{} '.format(bash_script)


def get_timeout(timeout):
    print('get_timeout|timeout : ', timeout)
    timeout_in_timedelta = timedelta(hours=timeout['hours'], minutes=timeout['minutes'], seconds=timeout['seconds'])
    print('get_timeout|timeout_in_timedelta : ', timeout_in_timedelta)
    return timeout_in_timedelta


def get_timeout_in_seconds(timeout):
    print('get_timeout_in_seconds|timeout : ', timeout)
    total_seconds = timeout['hours'] * 3600 + timeout['minutes'] * 60 + timeout['seconds']
    print('get_timeout_in_seconds|total_seconds : ', total_seconds)
    return total_seconds


def get_sensor_sql_query(model_type, model_rule_id):
    print('get_sensor_sql_query|[model_type, model_rule_id] : ', [model_type, model_rule_id])
    sql_query = None
    if model_type == 'hechms':
        sql_query = 'select TRUE from dss.hechms_rules where id={} and status in (3, 4)'.format(model_rule_id)
    elif model_type == 'flo2d':
        sql_query = 'select TRUE from dss.flo2d_rules where id={} and status in (3, 4)'.format(model_rule_id)
    elif model_type == 'decision':
        sql_query = 'select TRUE from dss.rule_definition where id={} and status in (3, 4)'.format(model_rule_id)
    return sql_query


def allowed_to_proceed(**context):
    print('allowed_to_proceed|params : ', context['params'])
    dag_rule_id = context['params']['id']
    print('allowed_to_proceed|dag_rule_id : ', dag_rule_id)
    if dag_rule_id is not None:
        adapter = get_dss_db_adapter()
        if adapter is not None:
            result = adapter.get_dynamic_dag_routing_status(dag_rule_id)
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
    else:
        print('Allowed to proceed')


def create_dag(dag_id, params, timeout, dag_tasks, default_args):
    print('create_dag|params : ', params)
    dag_rule_id = params['id']
    print('create_dag|dag_rule_id : ', dag_rule_id)
    dag = DAG(dag_id, catchup=False,
              dagrun_timeout=timeout,
              schedule_interval=None,
              params=params,
              on_failure_callback=on_dag_failure,
              default_args=default_args)

    with dag:
        init_task = PythonOperator(
            task_id='init_task',
            provide_context=True,
            python_callable=set_running_status,
            pool=dag_pool
        )

        task_list = [init_task]
        index = 0
        for dag_task in dag_tasks:
            print('create_dag|dag_task : ', dag_task)
            index += 1
            if dag_task['task_type'] == 1:
                task = BashOperator(
                    task_id=dag_task['task_name'],
                    bash_command=get_bash_command(dag_task['task_content'], dag_task['input_params'], dag),
                    execution_timeout=get_timeout(dag_task['timeout']),
                    on_failure_callback=on_dag_failure,
                    pool=dag_pool
                )
                task_list.append(task)

                checker = PythonOperator(
                    task_id='allow_checker_{}'.format(index),
                    provide_context=True,
                    python_callable=allowed_to_proceed,
                    pool=dag_pool
                )
                task_list.append(checker)

            elif dag_task['task_type'] == 2:

                print('create_dag|index : ', index)
                task = DynamicTriggerDagRunOperator(
                    task_id=dag_task['task_name'],
                    python_callable=create_trigger_dag_run,
                    execution_timeout=get_timeout(dag_task['timeout']),
                    on_failure_callback=on_dag_failure,
                    pool=dag_pool
                )
                task_list.append(task)

                checker = PythonOperator(
                    task_id='allow_checker_{}'.format(index),
                    provide_context=True,
                    python_callable=allowed_to_proceed,
                    pool=dag_pool
                )
                task_list.append(checker)

                wait = BashOperator(
                    task_id='wait_a_minute_task_{}'.format(index),
                    bash_command='sleep 60',
                    pool=dag_pool)
                task_list.append(wait)

                sensor = SqlSensor(
                    task_id='wait_for_{}_to_be_completed'.format(dag_task['task_name']),
                    conn_id='dss_conn',
                    sql=get_sensor_sql_query(dag_task['input_params']['model_type'],
                                             dag_task['input_params']['rule_id']),
                    poke_interval=60,
                    allow_null=False,
                    timeout=get_timeout_in_seconds(dag_task['timeout']),
                    pool=dag_pool)
                task_list.append(sensor)

                print('create_dag|timeout:', sensor.timeout)
                print('create_dag|dag_id', sensor.dag_id)
        end_task = PythonOperator(
            task_id='end_task',
            provide_context=True,
            python_callable=set_complete_status,
            pool=dag_pool
        )

        task_list.append(end_task)

        for i in range(len(task_list) - 1):
            task_list[i].set_downstream(task_list[i + 1])

    return dag


def generate_dynamic_workflow_dag(dss_adapter, dag_rule):
    print('generate_dynamic_workflow_dag|dag_rule : ', dag_rule)
    dag_tasks = get_dynamic_dag_tasks(dss_adapter, dag_rule['id'])
    if len(dag_tasks) > 0:
        timeout = get_timeout(dag_rule['timeout'])
        default_args = {
            'owner': 'dss admin',
            'start_date': datetime.utcnow(),
            'email': ['hasithadkr7@gmail.com'],
            'email_on_failure': True,
            'retries': 1,
            'retry_delay': timedelta(seconds=30)
        }
        params = dag_rule
        dag_id = dag_rule['dag_name']
        globals()[dag_id] = create_dag(dag_id, params,
                                       timeout,
                                       dag_tasks,
                                       default_args)


def start_creating():
    print('start_creating dynamic dags')
    db_config = Variable.get('db_config', deserialize_json=True)
    print('start_creating|db_config : ', db_config)
    adapter = RuleEngineAdapter.get_instance(db_config)
    routines = get_all_dynamic_dag_routines(adapter)
    if len(routines) > 0:
        for routine in routines:
            generate_dynamic_workflow_dag(adapter, routine)


start_creating()
