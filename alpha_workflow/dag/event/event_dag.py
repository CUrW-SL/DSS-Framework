import sys
from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/utils')
from curw_obs import CurwFcstAdapter

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/utils')
from mean_util import update_sub_basin_mean_rain

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/utils')
from db_util import RuleEngineAdapter

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/utils')
from mean_util import update_sub_basin_mean_rain

dag_pool = 'wrf_pool'
ssh_cmd_template = 'sshpass -p \'{}\' ssh {}@{} {}'


def update_workflow_status(status, rule_id):
    adapter = get_dss_db_adapter()
    if adapter is not None:
        adapter.update_wrf_rule_status(status, rule_id)
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


def get_rule_by_id(rule_id):
    db_adapter = get_dss_db_adapter()
    if db_adapter:
        wrf_rule = db_adapter.get_wrf_rule_info_by_id(rule_id)
        return wrf_rule
    else:
        print('db adapter error')
        return None


def set_running_status(**context):
    rule_id = ''
    print('set_running_status|rule_id :', rule_id)
    if rule_id is not None:
        update_workflow_status(2, rule_id)
    else:
        print('set_running_status|rule_id not found')


def set_complete_status(**context):
    rule_id = ''
    print('set_complete_status|rule_id :', rule_id)
    if rule_id is not None:
        update_workflow_status(3, rule_id)
    else:
        print('set_complete_status|rule_id not found')


def on_dag_failure(context):
    rule_id = ''
    print('on_dag_failure|rule_id : ', rule_id)
    if rule_id is not None:
        update_workflow_status(4, rule_id)
        print('on_dag_failure|set error status for rule|rule_id :', rule_id)
    else:
        print('on_dag_failure|rule_id not found')


def get_timeout(timeout):
    print('get_timeout|timeout : ', timeout)
    timeout_in_timedelta = timedelta(hours=timeout['hours'], minutes=timeout['minutes'], seconds=timeout['seconds'])
    print('get_timeout|timeout_in_timedelta : ', timeout_in_timedelta)
    return timeout_in_timedelta


def push_rule_to_xcom(dag_run, **kwargs):
    print('run_this_func|dag_run : ', dag_run)
    wrf_rule = dag_run.conf
    print('run_this_func|wrf_rule : ', wrf_rule)
    return wrf_rule


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
        init_wrf = PythonOperator(
            task_id='init_wrf',
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

        complete_state_wrf = PythonOperator(
            task_id='complete_state_wrf',
            provide_context=True,
            params=dag_rule,
            python_callable=set_complete_status,
            pool=dag_pool
        )

        init_wrf >> running_status >> complete_state_wrf

    return dag


def generate_wrf_workflow_dag(dag_rule):
    print('generate_wrf_workflow_dag|dag_rule : ', dag_rule)
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


def create_wrf_dags():
    db_config = Variable.get('db_config', deserialize_json=True)
    print('start_creating|db_config : ', db_config)
    adapter = RuleEngineAdapter.get_instance(db_config)
    rules = adapter.get_all_wrf_rules()
    print('start_wrf_creating|rules : ', rules)
    if len(rules) > 0:
        for rule in rules:
            try:
                generate_wrf_workflow_dag(rule)
            except Exception as e:
                print('generate_wrf_workflow_dag|Exception: ', str(e))


create_wrf_dags()
