from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

prod_dag_name = 'wrf_4_SE_dag'


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7.com'],
    'email_on_failure': True,
}

download_18hr_gfs_cmd = 'echo "download_18hr_gfs_cmd" ;sleep $[($RANDOM % 10) + 1]s'
run_wps4_cmd = 'echo "run_wps4_cmd" ;sleep $[($RANDOM % 10) + 1]s'
run_wrf4_SE_cmd = 'echo "run_wrf4_SE_cmd" ;sleep $[($RANDOM % 10) + 1]s'
extract_stations_cmd = 'echo "extract_stations_cmd" ;sleep $[($RANDOM % 10) + 1]s'
create_gsmap_cmd = 'echo "create_gsmap_cmd" ;sleep $[($RANDOM % 10) + 1]s'
extract_netcdf_weather_score_cmd = 'echo "extract_netcdf_weather_score_cmd" ;sleep $[($RANDOM % 10) + 1]s'
push_wrfv4_data_cmd = 'echo "push_wrfv4_data_cmd" ;sleep $[($RANDOM % 10) + 1]s'


def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run WRF v4 SE DSEG') as dag:

    init_wrfv4_SE = PythonOperator(
        task_id='init_wrfv4_SE',
        provide_context=True,
        python_callable=run_this_func,
        dag=dag,
    )

    run_wrf4_SE = BashOperator(
        task_id='run_wrf4_SE',
        bash_command=run_wrf4_SE_cmd,
    )

    extract_stations = BashOperator(
        task_id='extract_stations',
        bash_command=extract_stations_cmd,
    )

    create_gsmap = BashOperator(
        task_id='create_gsmap',
        bash_command=create_gsmap_cmd,
    )

    extract_netcdf_weather_score = BashOperator(
        task_id='extract_netcdf_weather_score',
        bash_command=extract_netcdf_weather_score_cmd,
    )

    push_wrfv4_data = BashOperator(
        task_id='create_rainfall',
        bash_command=push_wrfv4_data_cmd,
    )

    init_wrfv4_SE >> run_wrf4_SE >> extract_stations >> create_gsmap >> extract_netcdf_weather_score >> push_wrfv4_data

