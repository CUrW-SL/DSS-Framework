from datetime import datetime

from airflow import DAG

from airflow.operators.python_operator import PythonOperator


def create_dag(dag_id,
               schedule,
               dag_number,
               default_args):
    def hello_world_py(*args):
        print('Hello World')
        print('This is DAG: {}'.format(str(dag_number)))

    dag = DAG(dag_id, catchup=False,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        t1 = PythonOperator(
            task_id='hello_world',
            python_callable=hello_world_py,
            dag_number=dag_number)

    return dag


# build a dag for each number in range(3)
for n in range(1, 3):
    dag_id = 'hello_world_{}'.format(str(n))

    default_args = {'owner': 'dss_admin',
                    'start_date': datetime(2020, 1, 18)
                    }

    schedule = '*/10 * * * *'

    dag_number = n

    globals()[dag_id] = create_dag(dag_id,
                                   schedule,
                                   dag_number,
                                   default_args)
