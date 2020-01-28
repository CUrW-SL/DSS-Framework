from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import sys

sys.path.insert(0, '/home/curw/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/curw/git/DSS-Framework/gen_util')
from controller_util import get_all_external_bash_routines, get_dynamic_dag_tasks

dag_pool = 'external_dag_pool'


def update_workflow_status(status, rule_id):
    try:
        db_config = Variable.get('db_config', deserialize_json=True)
        try:
            adapter = RuleEngineAdapter.get_instance(db_config)
            adapter.update_external_bash_routing_status(status, rule_id)
        except Exception as ex:
            print('update_workflow_status|db_adapter|Exception: ', str(ex))
    except Exception as e:
        print('update_workflow_status|db_config|Exception: ', str(e))


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


def create_dag(dag_id, params, timeout, dag_tasks, default_args):
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
        for dag_task in dag_tasks:
            task = BashOperator(
                task_id=dag_task['task_name'],
                bash_command=get_bash_command(dag_task['bash_script'], dag_task['input_params']),
                execution_timeout=get_timeout(dag_task['timeout']),
                on_failure_callback=on_dag_failure,
                pool=dag_pool
            )
            task_list.append(task)

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


# example bash command : /home/uwcc-admin/calculate.sh -a 23 -date '2020-01-11' -c 1.4
def get_bash_command(bash_script, input_params):
    print('get_bash_command|bash_script : ', bash_script)
    print('get_bash_command|input_params : ', input_params)
    inputs = []
    if input_params:
        for key in input_params.keys():
            inputs.append('-{} {}'.format(key, input_params[key]))
        if len(inputs) > 0:
            input_str = ' '.join(inputs)
            return '{} {}'.format(bash_script, input_str)
        else:
            return '{} '.format(bash_script)
    else:
        return '{} '.format(bash_script)


def get_timeout(timeout):
    print('get_timeout|timeout : ', timeout)
    return timedelta(hours=timeout['hours'], minutes=timeout['minutes'], seconds=timeout['seconds'])


def generate_external_bash_dag(dss_adapter, dag_rule):
    print('generate_external_bash_dag|dag_rule : ', dag_rule)
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
    routines = get_all_external_bash_routines(adapter)
    if len(routines) > 0:
        for routine in routines:
            generate_external_bash_dag(adapter, routine)


start_creating()
