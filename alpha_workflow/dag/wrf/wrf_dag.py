import subprocess
from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import sys

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/utils')
from db_util import RuleEngineAdapter

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/plugins/operators')
from gfs_sensor import GfsSensorOperator


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
    rule_id = get_rule_id(context)
    print('set_running_status|rule_id :', rule_id)
    if rule_id is not None:
        update_workflow_status(2, rule_id)
    else:
        print('set_running_status|rule_id not found')


def set_complete_status(**context):
    rule_id = get_rule_id(context)
    print('set_complete_status|rule_id :', rule_id)
    if rule_id is not None:
        update_workflow_status(3, rule_id)
    else:
        print('set_complete_status|rule_id not found')


def on_dag_failure(context):
    rule_id = get_rule_id(context)
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


def allowed_to_proceed(rule_id):
    print('allowed_to_proceed|rule_id : ', rule_id)
    if rule_id is not None:
        adapter = get_dss_db_adapter()
        if adapter is not None:
            result = adapter.get_wrf_rule_status_by_id(rule_id)
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


def get_execusion_date(context):
    rule = context['task_instance'].xcom_pull(task_ids='init_wrf')
    if rule is not None:
        if 'run_date' in rule:
            exec_datetime_str = rule['run_date']
            exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S')
            exec_date = exec_datetime.strftime('%Y-%m-%d')
        else:
            exec_datetime_str = context["execution_date"].to_datetime_string()
            exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S')
            exec_date = exec_datetime.strftime('%Y-%m-%d')
    return exec_date


def get_execution_date_time(context):
    rule = context['task_instance'].xcom_pull(task_ids='init_wrf')
    if rule is not None:
        if 'run_date' in rule:
            exec_datetime_str = rule['run_date']
        else:
            exec_datetime_str = context["execution_date"].to_datetime_string()
    return exec_datetime_str


def get_wrf_run_command(**context):
    wrf_rule_id = get_rule_id(context)
    db_config = Variable.get('db_config', deserialize_json=True)
    db_user = db_config['mysql_user']
    db_password = db_config['mysql_password']
    db_name = db_config['mysql_db']
    db_host = db_config['mysql_host']
    vm_config = Variable.get('ubuntu1_config', deserialize_json=True)
    vm_user = vm_config['user']
    vm_password = vm_config['password']
    print('get_wrf_run_command|wrf_rule_id : ', wrf_rule_id)
    allowed_to_proceed(wrf_rule_id)
    wrf_rule = get_rule_by_id(wrf_rule_id)
    if wrf_rule is not None:
        wrf_model = wrf_rule['target_model']
        wrf_version = wrf_rule['version']
        wrf_run = wrf_rule['run']
        gfs_hour = wrf_rule['hour']
        namelist_wps_id = wrf_rule['namelist_wps']
        namelist_input_id = wrf_rule['namelist_input']
        print('get_wrf_run_command|rule_details: ', wrf_rule['rule_details'])
        run_node = wrf_rule['rule_details']['run_node']
        run_script = wrf_rule['rule_details']['run_script']
        exec_date = get_execusion_date(context)
        if db_config is not None:
            run_script = '{}  -r {} -m {} -v {} -h {} -a {} -b {} -p {} -q {} -t {} -s {} -d {}'.format(run_script, wrf_run,
                                                                                                        wrf_model,
                                                                                                        wrf_version,
                                                                                                        gfs_hour,
                                                                                                        namelist_wps_id,
                                                                                                        namelist_input_id,
                                                                                                        db_user,
                                                                                                        db_password,
                                                                                                        db_name, db_host,
                                                                                                        exec_date)
            print('get_wrf_run_command|run_script : ', run_script)
            run_wrf_cmd = ssh_cmd_template.format(vm_password, vm_user, run_node, run_script)
            print('get_wrf_run_command|run_wrf_cmd : ', run_wrf_cmd)
            subprocess.call(run_wrf_cmd, shell=True)
    else:
        raise AirflowException('wrf rule not found')


def get_push_command(**context):
    wrf_rule_id = get_rule_id(context)
    print('get_wrf_run_command|wrf_rule_id : ', wrf_rule_id)
    allowed_to_proceed(wrf_rule_id)
    wrf_rule = get_rule_by_id(wrf_rule_id)
    if wrf_rule is not None:
        print('get_wrf_run_command|wrf_rule : ', wrf_rule)
        wrf_model = wrf_rule['target_model']
        rule_name = wrf_rule['name']
        wrf_run = wrf_rule['run']
        gfs_hour = wrf_rule['hour']
        vm_config = Variable.get('ubuntu1_config', deserialize_json=True)
        vm_user = vm_config['user']
        vm_password = vm_config['password']
        print('get_wrf_run_command|rule_details: ', wrf_rule['rule_details'])
        push_node = wrf_rule['rule_details']['push_node']
        bash_script = wrf_rule['rule_details']['push_script']
        push_config = wrf_rule['rule_details']['push_config']
        wrf_bucket = wrf_rule['rule_details']['wrf_bucket']
        exec_date = get_execusion_date(context)
        push_script = '{} {} {} d{} {} {} {} {}'.format(bash_script, push_config, wrf_bucket, wrf_run,
                                                     gfs_hour, wrf_model, exec_date, rule_name)
        print('get_push_command|run_script : ', push_script)
        push_wrf_cmd = ssh_cmd_template.format(vm_password, vm_user, push_node, push_script)
        print('get_push_command|push_wrf_cmd : ', push_wrf_cmd)
        subprocess.call(push_wrf_cmd, shell=True)
    else:
        raise AirflowException('wrf rule not found')


def push_rule_to_xcom(dag_run, **kwargs):
    print('run_this_func|dag_run : ', dag_run)
    wrf_rule = dag_run.conf
    print('run_this_func|wrf_rule : ', wrf_rule)
    return wrf_rule


def get_rule_id(context):
    rule_id = context['task_instance'].xcom_pull(task_ids='init_wrf')['id']
    if rule_id:
        print('get_rule_id|rule_id : ', rule_id)
        return rule_id
    else:
        return None


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

        check_gfs_availability_wrf = GfsSensorOperator(
            task_id='check_gfs_availability_wrf',
            poke_interval=60,
            execution_timeout=timedelta(minutes=45),
            params=dag_rule,
            provide_context=True,
            pool=dag_pool
        )

        run_wrf = PythonOperator(
            task_id='run_wrf',
            provide_context=True,
            execution_timeout=timedelta(hours=8, minutes=30),
            params=dag_rule,
            python_callable=get_wrf_run_command,
            pool=dag_pool
        )

        wrf_data_push_wrf = PythonOperator(
            task_id='wrf_data_push_wrf',
            provide_context=True,
            params=dag_rule,
            python_callable=get_push_command,
            pool=dag_pool
        )

        complete_state_wrf = PythonOperator(
            task_id='complete_state_wrf',
            provide_context=True,
            params=dag_rule,
            python_callable=set_complete_status,
            pool=dag_pool
        )

        init_wrf >> running_status >> check_gfs_availability_wrf >> \
        run_wrf >> wrf_data_push_wrf >> complete_state_wrf

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
