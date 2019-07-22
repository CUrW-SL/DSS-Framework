import pprint
from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

pp = pprint.PrettyPrinter(indent=4)

args = {
    'start_date': datetime.utcnow(),
    'owner': 'dss admin',
}

dag = DAG(
    dag_id='dss_trigger_target_dag',
    default_args=args,
    schedule_interval=None,
)


def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))


run_this = PythonOperator(
    task_id='target_run_this',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag,
)

# You can also access the DagRun object in templates
# bash_task = BashOperator(
#     task_id="target_bash_task",
#     bash_command='echo "Here is the message: '
#                  '{{ dag_run.conf["message"] if dag_run else "" }}" ',
#     dag=dag,
# )

