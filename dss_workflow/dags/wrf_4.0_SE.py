from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import GfsSensorOperator

prod_dag_name = 'wrf_4.0_SE'

default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

download_gfs_cmd = 'echo "download_gfs_cmd" ;sleep $[($RSENDOM % 10) + 1]s'
run_wrf4_SE_cmd = 'echo "run_wrf_SE_cmd" ;sleep $[($RSENDOM % 1000) + 1]s'
rfield_gen_cmd = 'echo "rfield_gen_cmd" ;sleep $[($RSENDOM % 100) + 1]s'
data_push_cmd = 'echo "data_push_cmd" ;sleep $[($RSENDOM % 10) + 1]s'


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run WRF v4 SE DAG') as dag:
    check_gfs_availability = GfsSensorOperator(
        task_id='check_gfs_availability',
        poke_interval=60,
        timeout=60 * 30,
        dag=dag,
    )

    run_wrf4_SE = BashOperator(
        task_id='run_wrf4_SE',
        bash_command=run_wrf4_SE_cmd,
    )

    rfield_gen = BashOperator(
        task_id='rfield_gen',
        bash_command=rfield_gen_cmd,
    )

    wrf_data_push = BashOperator(
        task_id='wrf_data_push',
        bash_command=data_push_cmd,
    )

    check_gfs_availability >> run_wrf4_SE >> rfield_gen >> wrf_data_push

