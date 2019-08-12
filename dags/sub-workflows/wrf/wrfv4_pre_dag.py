from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

prod_dag_name = 'wrfv4-pre'


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7.com'],
    'email_on_failure': True,
}

download_18hr_gfs_cmd = 'echo "download_18hr_gfs_cmd" ; sleep $[ ( $RANDOM % 10 )  + 1 ]s'
run_wps4_cmd = 'echo "run_wps4_cmd" ; sleep $[($RANDOM % 10) + 1]s'


def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run WRF v4 A DAG') as dag:

    init_wrfv4_pre = PythonOperator(
        task_id='init_wrfv4_pre',
        provide_context=True,
        python_callable=run_this_func,
        dag=dag,
    )

    download_18hr_gfs = BashOperator(
        task_id='download_18hr_gfs',
        bash_command=download_18hr_gfs_cmd,
    )

    run_wps4 = BashOperator(
        task_id='run_wps4',
        bash_command=run_wps4_cmd,
    )

    init_wrfv4_pre >> download_18hr_gfs >> run_wps4

