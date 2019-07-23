from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


prod_dag_name = 'flo2d-150m-dag'
queue = 'default'
dag_pool = 'curw_prod_runs'


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'queue': queue,
}

create_raincell_cmd = 'echo "create_raincell_cmd"'

create_inflow_cmd = 'echo "create_inflow_cmd"'

create_outflow_cmd = 'echo "create_outflow_cmd"'

run_flo2d_150m_cmd = 'echo "run_flo2d_150m_cmd"'

extract_water_level_cmd = 'echo "extract_water_level_cmd"'

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run Flo2d 150m DAG') as dag:
    create_raincell = BashOperator(
        task_id='create_raincell',
        bash_command=create_raincell_cmd,
        pool=dag_pool,
    )

    create_inflow = BashOperator(
        task_id='create_inflow',
        bash_command=create_inflow_cmd,
        pool=dag_pool,
    )

    create_outflow = BashOperator(
        task_id='create_outflow',
        bash_command=create_outflow_cmd,
        pool=dag_pool,
    )

    run_flo2d_150m = BashOperator(
        task_id='run_flo2d_150m',
        bash_command=run_flo2d_150m_cmd,
        pool=dag_pool,
    )

    extract_water_level = BashOperator(
        task_id='extract_water_level',
        bash_command=extract_water_level_cmd,
        pool=dag_pool,
    )

    create_raincell >> create_inflow >> create_outflow >> run_flo2d_150m >> extract_water_level

