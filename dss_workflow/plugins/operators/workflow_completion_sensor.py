from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from datetime import datetime, timedelta
from ftplib import FTP


def check_completion(model, rule_id):
    completed = False
    print('check_completion|[model, rule_id] : ', [model, rule_id])
    return completed


class WorkflowSensorOperator(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(WorkflowSensorOperator, self).__init__(*args, **kwargs)

    def poke(self, context):
        try:
            print('-----------------------------------------------------------------------')
            task_info = context['task_instance'].xcom_pull(task_ids='init_wrfv4')
            print('GfsSensorOperator|task_info : ', task_info)
            rule_info = context['task_instance'].xcom_pull(task_ids='init_wrfv4')['rule_info']
            print('GfsSensorOperator|rule_info : ', rule_info)

            condition = check_completion(gfs_hour, wrf_run)
            print('WorkflowSensorOperator|condition : ', condition)
            print('-----------------------------------------------------------------------')
            return condition
        except AirflowSensorTimeout as to:
            self._do_skip_downstream_tasks(context)
            raise AirflowSensorTimeout('WorkflowSensorOperator. Time is OUT.')


class MyFirstPlugin(AirflowPlugin):
    name = "workflow_sensor"
    operators = [WorkflowSensorOperator]
