from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


prod_dag_name = 'hec-hms-distributed-dag'
queue = 'default'
dag_pool = 'curw_prod_runs'


default_args = {
    'owner': 'curwsl admin',
    'start_date': datetime.utcnow(),
    'queue': queue,
}

create_rainfall_cmd = 'echo "create_rainfall_cmd"'

run_hechms_distributed_cmd = 'echo "run_hechms_distributed_cmd"'

upload_discharge_cmd = 'echo "upload_discharge_cmd"'

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run HecHms DAG') as dag:

    create_rainfall = BashOperator(
        task_id='create_rainfall',
        bash_command=create_rainfall_cmd,
        pool=dag_pool,
    )

    run_hechms_distributed = BashOperator(
        task_id='run_hechms_distributed',
        bash_command=run_hechms_distributed_cmd,
        pool=dag_pool,
    )

    upload_discharge = BashOperator(
        task_id='upload_discharge',
        bash_command=upload_discharge_cmd,
        pool=dag_pool,
    )

    create_rainfall >> run_hechms_distributed >> upload_discharge

