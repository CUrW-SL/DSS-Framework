from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

prod_dag_name = 'hechms_distributed_dag'


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7.com'],
    'email_on_failure': True,
}

create_rainfall_cmd = 'echo "create_rainfall_cmd" ;sleep $[($RANDOM % 10) + 1]s'
run_hechms_distributed_cmd = 'echo "run_hechms_distributed_cmd" ;sleep $[($RANDOM % 10) + 1]s'
upload_discharge_cmd = 'echo "upload_discharge_cmd" ;sleep $[($RANDOM % 10) + 1]s'


def run_this_func(ds, **kwargs):
    print('hechms_distributed_dag|run_this_func|kwargs : ', kwargs)
    print("Remotely received value of {} for key=payload".
          format(kwargs['dag_run'].conf['payload']))


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run HecHms DAG') as dag:

    init_hec_distributed = PythonOperator(
        task_id='init_hec_distributed',
        provide_context=True,
        python_callable=run_this_func,
    )

    create_rainfall = BashOperator(
        task_id='create_rainfall',
        bash_command=create_rainfall_cmd,
    )

    run_hechms_distributed = BashOperator(
        task_id='run_hechms_distributed',
        bash_command=run_hechms_distributed_cmd,
    )

    upload_discharge = BashOperator(
        task_id='upload_discharge',
        bash_command=upload_discharge_cmd,
    )

    init_hec_distributed >> create_rainfall >> run_hechms_distributed >> upload_discharge

