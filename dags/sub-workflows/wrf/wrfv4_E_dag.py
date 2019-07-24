from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

prod_dag_name = 'wrfv4-E-dag'
queue = 'default'
dag_pool = 'curw_prod_runs'


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'queue': queue,
    'email': ['hasithadkr7.com'],
    'email_on_failure': True,
}

download_18hr_gfs_cmd = 'echo "download_18hr_gfs_cmd"'
run_wps4_cmd = 'echo "run_wps4_cmd"'
run_wrf4_E_cmd = 'echo "run_wrf4_E_cmd"'
extract_stations_cmd = 'echo "extract_stations_cmd"'
create_gsmap_cmd = 'echo "create_gsmap_cmd"'
extract_netcdf_weather_score_cmd = 'echo "extract_netcdf_weather_score_cmd"'
push_wrfv4_data_cmd = 'echo "push_wrfv4_data_cmd"'


def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run WRF v4 E DEG') as dag:

    init_wrfv4_E = PythonOperator(
        task_id='init_wrfv4_E',
        provide_context=True,
        python_callable=run_this_func,
        dag=dag,
    )

    run_wrf4_E = BashOperator(
        task_id='run_wrf4_E',
        bash_command=run_wrf4_E_cmd,
        pool=dag_pool,
    )

    extract_stations = BashOperator(
        task_id='extract_stations',
        bash_command=extract_stations_cmd,
        pool=dag_pool,
    )

    create_gsmap = BashOperator(
        task_id='create_gsmap',
        bash_command=create_gsmap_cmd,
        pool=dag_pool,
    )

    extract_netcdf_weather_score = BashOperator(
        task_id='extract_netcdf_weather_score',
        bash_command=extract_netcdf_weather_score_cmd,
        pool=dag_pool,
    )

    push_wrfv4_data = BashOperator(
        task_id='create_rainfall',
        bash_command=push_wrfv4_data_cmd,
        pool=dag_pool,
    )

    init_wrfv4_E >> run_wrf4_E >> extract_stations >> create_gsmap >> extract_netcdf_weather_score >> push_wrfv4_data

