from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import GfsSensorOperator
from airflow.operators.python_operator import PythonOperator

prod_dag_name = 'wrf_4.0_A'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

#./runner.sh -r 0 -m E -v 4.0 -h 18 -d 2019-10-24
#./rfielder.sh -r 0 -m E -v 4.0 -h 18

download_gfs_cmd = 'echo "download_gfs_cmd" ;sleep $[($RANDOM % 10) + 1]s'
run_wrf4_A_cmd = 'echo "run_wrf_A_cmd" ;sleep $[($RANDOM % 1000) + 1]s'
rfield_gen_cmd = 'echo "rfield_gen_cmd" ;sleep $[($RANDOM % 100) + 1]s'
data_push_cmd = 'echo "data_push_cmd" ;sleep $[($RANDOM % 10) + 1]s'


def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=payload".
          format(kwargs['dag_run'].conf['payload']))
    #{'dag_name': dag_name, 'payload': payload}
    rule_info = kwargs['dag_run'].conf['payload']
    print('rule_info : ', rule_info)
    wrf_rule = {'model': 'A', 'version': '4.0', 'rule_info': rule_info}
    return wrf_rule


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run WRF v4 A DAG') as dag:
    init_wrfv4_A = PythonOperator(
        task_id='init_wrfv4_A',
        provide_context=True,
        python_callable=run_this_func,
        dag=dag,
    )

    check_gfs_availability = GfsSensorOperator(
        task_id='check_gfs_availability',
        poke_interval=60,
        timeout=60 * 30,
        dag=dag,
    )

    run_wrf4_A = BashOperator(
        task_id='run_wrf4_A',
        bash_command=run_wrf4_A_cmd,
    )

    rfield_gen = BashOperator(
        task_id='rfield_gen',
        bash_command=rfield_gen_cmd,
    )

    wrf_data_push = BashOperator(
        task_id='wrf_data_push',
        bash_command=data_push_cmd,
    )

    init_wrfv4_A >> check_gfs_availability >> run_wrf4_A >> rfield_gen >> wrf_data_push

