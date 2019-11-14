from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
import sys

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter


def check_completion(model, rule_id):
    completed = False
    print('check_completion|[model, rule_id] : ', [model, rule_id])
    db_config = Variable.get('db_config', deserialize_json=True)
    try:
        adapter = RuleEngineAdapter.get_instance(db_config)
        if model == 'wrf':
            print('WRF')
            wrf_rule = adapter.get_wrf_rule_status_by_id(rule_id)
            if wrf_rule is not None:
                wrf_rule_status = wrf_rule['status']
                if wrf_rule_status == 3 or wrf_rule_status == '3':
                    completed = True
        elif model == 'hechms':
            print('HecHms')
            hechms_rule = adapter.get_hechms_rule_status_by_id(rule_id)
            if hechms_rule is not None:
                hechms_rule_rule_status = hechms_rule['status']
                if hechms_rule_rule_status == 3 or hechms_rule_rule_status == '3':
                    completed = True
        elif model == 'flo2d':
            print('Flo2d')
            flo2d_rule = adapter.get_hechms_rule_status_by_id(rule_id)
            if flo2d_rule is not None:
                flo2d_rule_rule_status = flo2d_rule['status']
                if flo2d_rule_rule_status == 3 or flo2d_rule_rule_status == '3':
                    completed = True
    except Exception as ex:
        print('update_workflow_status|db_adapter|Exception: ', str(ex))
    return completed


class WorkflowSensorOperator(BaseSensorOperator):
    @apply_defaults
    def __init__(self, params, *args, **kwargs):
        self.init_task_id = params['init_task_id']
        self.model = params['model']
        super(WorkflowSensorOperator, self).__init__(*args, **kwargs)

    def poke(self, context):
        try:
            print('-----------------------------------------------------------------------')
            rule_info = context['task_instance'].xcom_pull(task_ids=self.init_task_id)['rule_info']
            print('WorkflowSensorOperator|rule_info : ', rule_info)
            rule_id = rule_info['id']
            condition = check_completion(self.model, rule_id)
            print('WorkflowSensorOperator|condition : ', condition)
            print('-----------------------------------------------------------------------')
            return condition
        except AirflowSensorTimeout as to:
            self._do_skip_downstream_tasks(context)
            raise AirflowSensorTimeout('WorkflowSensorOperator. Time is OUT.')


class MyFirstPlugin(AirflowPlugin):
    name = "workflow_sensor"
    operators = [WorkflowSensorOperator]
