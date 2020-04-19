import subprocess
from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
import sys
from airflow.models import XCom

sys.path.insert(0, '/home/curw/git/DSS-Framework/alpha_workflow/utils')
from db_util import RuleEngineAdapter

sys.path.insert(0, '/home/curw/git/DSS-Framework/accuracy_unit/wrf')
from mean_calc import calculate_wrf_model_mean

dag_pool = 'decision_pool'
prod_dag_name = 'decision_dag'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
WRF_MODEL_MAP = {'A':19,'C':20,'E':21,'SE':22}
# {'model_type':'decision_unit',
# 'decision_type':'event',
# 'decision_model':'wrf',
# 'rule_ids':[1,2,3,4]}


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


def get_wrf_rules():
    dss_adapter = get_dss_db_adapter()
    wrf_rules = dss_adapter.get_all_wrf_rules()
    rule_names = [wrf_rule['name'] for wrf_rule in wrf_rules]
    print('get_wrf_rules|rule_names : ', rule_names)
    return rule_names


def get_hechms_rules():
    dss_adapter = get_dss_db_adapter()
    hechms_rules = dss_adapter.get_all_hechms_rules()
    rule_names = [hechms_rule['name'] for hechms_rule in hechms_rules]
    print('get_hechms_rules|rule_names : ', rule_names)
    return rule_names


def decide_run_purpose(**context):
    print('decide_run_purpose|context : ', context)
    decision_config = context['task_instance'].xcom_pull(task_ids='init_task')
    decision_type = decision_config['decision_type']
    if decision_type == 'event':
        return 'event_flow'
    else:
        return 'production_flow'


def select_decision_model(**context):
    print('select_decision_model|context : ', context)
    decision_config = context['task_instance'].xcom_pull(task_ids='init_task')
    decision_model = decision_config['decision_model']
    if decision_model == 'wrf':
        return 'wrf_flow'
    else:
        return 'hechms_flow'


def push_decision_config_to_xcom(dag_run, **kwargs):
    decision_config = dag_run.conf
    print('push_decision_config_to_xcom|decision_config : ', decision_config)
    return decision_config


def get_decision_config(context):
    decision_config = context['task_instance'].xcom_pull(task_ids='init_task')
    print('get_decision_config|decision_config : ', decision_config)
    return decision_config


def get_rule_name(context):
    task_id = context['task'].task_id
    print('get_rule_name|task_id : ', task_id)
    rule_name = task_id[:len(task_id) - 5]
    print('get_rule_name|rule_name : ', rule_name)
    return rule_name


def wrf_models_decision(**context):
    print('wrf_models_decision|context:', context)
    dss_adapter = get_dss_db_adapter()
    rule_names = dss_adapter.get_wrf_rule_names()
    print('wrf_models_decision|rule_names : ', rule_names)
    if len(rule_names) > 0:
        for rule_name in rule_names:
            task_id = '{}_task'.format(rule_name)
            print('wrf_models_decision|task_id : ', task_id)
            # rmse_params = context['ti'].xcom_pull(task_ids=task_id)
            task_instance = context['task_instance']
            rmse_params = task_instance.xcom_pull(task_id, key=rule_name)
            print('wrf_models_decision|rmse_params : ', rmse_params)
            # print('wrf_models_decision|rmse_params : ', rmse_params)


def evaluate_wrf_model(**context):
    print('evaluate_wrf_model|context:', context)
    rule_name = get_rule_name(context)
    decision_config = get_decision_config(context)
    dss_adapter = get_dss_db_adapter()
    wrf_rule = dss_adapter.get_wrf_rule_info_by_name(rule_name)
    if wrf_rule is not None:
        target_model = wrf_rule['target_model']
        if decision_config['decision_type'] == 'production':
            print('evaluate_wrf_model|production')
        elif decision_config['decision_type'] == 'event':
            run_date = decision_config['run_date']
            start_limit = run_date
            run_date = datetime.strptime(run_date, DATE_TIME_FORMAT)
            end_limit = run_date + timedelta(days=1)
            end_limit = end_limit.strftime(DATE_TIME_FORMAT)
            sim_tag = 'gfs_d0_18'
            wrf_model_id = WRF_MODEL_MAP[target_model]
            print('evaluate_wrf_model|event|[wrf_model_id,sim_tag : ', [wrf_model_id,sim_tag])
            mean_calc = calculate_wrf_model_mean(sim_tag, wrf_model_id, start_limit, end_limit)
            print('evaluate_wrf_model|event|mean_calc : ', mean_calc)
            task_instance = context['task_instance']
            print('evaluate_wrf_model|event|task_instance : ', task_instance)
            task_instance.xcom_push(rule_name, mean_calc)
            # task_instance.xcom_push(key=rule_name, value=mean_calc)


def hechms_models_decision(**context):
    print('hechms_models_decision|context:', context)


def evaluate_hechms_model(**context):
    print('evaluate_hechms_model|context:', context)


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run Decision Making DAG', dagrun_timeout=timedelta(minutes=10),
         catchup=False) as dag:
    init_task = PythonOperator(
        task_id='init_task',
        provide_context=True,
        python_callable=push_decision_config_to_xcom,
        pool=dag_pool
    )

    run_purpose_branch = BranchPythonOperator(
        task_id='run_purpose_branch',
        provide_context=True,
        python_callable=decide_run_purpose,
        trigger_rule='all_done',
        dag=dag)

    production_flow = DummyOperator(
        task_id='production_flow',
        pool=dag_pool
    )

    event_flow = DummyOperator(
        task_id='event_flow',
        pool=dag_pool
    )

    model_selection_branch = BranchPythonOperator(
        task_id='model_selection_branch',
        provide_context=True,
        python_callable=select_decision_model,
        trigger_rule='none_failed',
        dag=dag)

    wrf_flow = DummyOperator(
        task_id='wrf_flow',
        pool=dag_pool
    )

    hechms_flow = DummyOperator(
        task_id='hechms_flow',
        pool=dag_pool
    )

    init_task >> run_purpose_branch >> [production_flow, event_flow]
    production_flow >> model_selection_branch >> [wrf_flow, hechms_flow]
    event_flow >> model_selection_branch >> [wrf_flow, hechms_flow]

    wrf_decision = PythonOperator(
        task_id='wrf_decision',
        provide_context=True,
        python_callable=wrf_models_decision,
        trigger_rule='none_failed',
        pool=dag_pool
    )

    hechms_decision = PythonOperator(
        task_id='hechms_decision',
        provide_context=True,
        python_callable=hechms_models_decision,
        trigger_rule='none_failed',
        pool=dag_pool
    )

    for wrf_rule_name in get_wrf_rules():
        wrf_rule = PythonOperator(
            task_id='{}_task'.format(wrf_rule_name),
            provide_context=True,
            python_callable=evaluate_wrf_model,
            pool=dag_pool
        )
        wrf_flow >> wrf_rule >> wrf_decision

    for hechms_rule_name in get_hechms_rules():
        hechms_rule = PythonOperator(
            task_id='{}_task'.format(hechms_rule_name),
            provide_context=True,
            python_callable=evaluate_hechms_model,
            pool=dag_pool
        )
        hechms_flow >> hechms_rule >> hechms_decision

    end_task = DummyOperator(
        task_id='end_task',
        trigger_rule='none_failed',
        pool=dag_pool
    )

    wrf_decision >> end_task
    hechms_decision >> end_task

