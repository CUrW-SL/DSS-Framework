from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


prod_dag_name = 'wrfv4-SE-dag'
queue = 'default'
dag_pool = 'curw_prod_runs'


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'queue': queue,
}

download_18hr_gfs_cmd = 'echo "download_18hr_gfs_cmd"'
run_wps4_cmd = 'echo "run_wps4_cmd"'
run_wrf4_SE_cmd = 'echo "run_wrf4_SE_cmd"'
extract_stations_cmd = 'echo "extract_stations_cmd"'
create_gsmap_cmd = 'echo "create_gsmap_cmd"'
extract_netcdf_weather_score_cmd = 'echo "extract_netcdf_weather_score_cmd"'
push_wrfv4_data_cmd = 'echo "push_wrfv4_data_cmd"'


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run WRF v4 SE DSEG') as dag:

    download_18hr_gfs = BashOperator(
        task_id='download_18hr_gfs',
        bash_command=download_18hr_gfs_cmd,
        pool=dag_pool,
    )

    run_wps4 = BashOperator(
        task_id='run_wps4',
        bash_command=run_wps4_cmd,
        pool=dag_pool,
    )

    run_wrf4_SE = BashOperator(
        task_id='run_wrf4_SE',
        bash_command=run_wrf4_SE_cmd,
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

    download_18hr_gfs >> run_wps4 >> run_wrf4_SE >> extract_stations >> create_gsmap >> extract_netcdf_weather_score >> push_wrfv4_data

