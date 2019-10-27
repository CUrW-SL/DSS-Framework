from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

prod_dag_name = 'hechms_single'


default_args = {
    'owner': 'dss admin',
    'start_date': datetime.utcnow(),
    'email': ['hasithadkr7@gmail.com'],
    'email_on_failure': True,
}

create_rainfall_cmd = 'echo "create_rainfall_cmd" ;sleep $[($RANDOM % 10) + 1]s'
run_hechms_single_cmd = 'echo "run_hechms_single_cmd" ;sleep $[($RANDOM % 10) + 1]s'
upload_discharge_cmd = 'echo "upload_discharge_cmd" ;sleep $[($RANDOM % 10) + 1]s'


def run_this_func(dag_run, **kwargs):
    print('run_this_func|dag_run : ', dag_run)
    hechms_rule = {'model': 'single', 'rule_info': dag_run.conf}
    print('run_this_func|hechms_rule : ', hechms_rule)
    return hechms_rule


with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=None,
         description='Run HecHms DAG') as dag:

    init_hec_single = PythonOperator(
        task_id='init_hec_single',
        provide_context=True,
        python_callable=run_this_func,
    )

    create_rainfall = BashOperator(
        task_id='create_rainfall',
        bash_command=create_rainfall_cmd,
    )

    run_hechms_single = BashOperator(
        task_id='run_hechms_single',
        bash_command=run_hechms_single_cmd,
    )

    upload_discharge = BashOperator(
        task_id='upload_discharge',
        bash_command=upload_discharge_cmd,
    )

    init_hec_single >> create_rainfall >> run_hechms_single >> upload_discharge

