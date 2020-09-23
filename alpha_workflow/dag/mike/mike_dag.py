import airflow
from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
import requests

schedule_interval = '10 * * * *'
prod_dag_name = 'mike_dag'
dag_pool = 'mike_pool'

ROUTER_IP = '124.43.13.195'
MIKE_WIN_PORT = '8081'
MATLAB_WIN_PORT = '8082'

default_args = {
    'owner': 'dss admin',
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['hasithadkr7gmail.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# curl -X GET "http://124.43.13.195:8082/input-process?run_date=2020-09-21&run_time=08:00:00"

input_process_cmd_request = 'http://{}:{}/input-process?' \
                            'run_date={}&run_time={} '

model_run_cmd_request = 'http://{}:{}/model-run?' \
                            'run_date={}&run_time={} '

output_process_cmd_request = 'http://{}:{}/output-process?' \
                            'run_date={}&run_time={} '


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


def get_local_exec_date_time_from_context(context):
    exec_datetime_str = context["execution_date"].to_datetime_string()
    exec_datetime = datetime.strptime(exec_datetime_str, '%Y-%m-%d %H:%M:%S') + timedelta(hours=5, minutes=30)
    exec_date = exec_datetime.strftime('%Y-%m-%d')
    exec_time = exec_datetime.strftime('%H:00:00')
    return [exec_date, exec_time]


def get_input_process_cmd(**context):
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    request_url = input_process_cmd_request.format(ROUTER_IP, MATLAB_WIN_PORT, exec_date, exec_time)
    print('get_input_process_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_input_process_cmd|success')
    else:
        raise AirflowException(
            'get_input_process_cmd|failed'
        )


def get_model_run_cmd(**context):
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    request_url = model_run_cmd_request.format(ROUTER_IP, MATLAB_WIN_PORT, exec_date, exec_time)
    print('get_model_run_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_model_run_cmd|success')
    else:
        raise AirflowException(
            'get_model_run_cmd|failed'
        )


def get_output_process_cmd(**context):
    [exec_date, exec_time] = get_local_exec_date_time_from_context(context)
    request_url = output_process_cmd_request.format(ROUTER_IP, MATLAB_WIN_PORT, exec_date, exec_time)
    print('get_output_process_cmd|request_url : ', request_url)
    if send_http_get_request(request_url):
        print('get_output_process_cmd|success')
    else:
        raise AirflowException(
            'get_output_process_cmd|failed'
        )


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run MIKE DAG', catchup=False) as dag:
    input_process = PythonOperator(
        task_id='input_process',
        provide_context=True,
        python_callable=get_input_process_cmd,
        pool=dag_pool
    )

    model_run = PythonOperator(
        task_id='model_run',
        provide_context=True,
        python_callable=get_model_run_cmd,
        pool=dag_pool
    )

    output_process = PythonOperator(
        task_id='output_process',
        provide_context=True,
        python_callable=get_output_process_cmd,
        pool=dag_pool
    )

    input_process >> model_run >> output_process

