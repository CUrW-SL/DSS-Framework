from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
import sys

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter


class DssUnitSensorOperator(BaseSensorOperator):
    @apply_defaults
    def __init__(self, params, *args, **kwargs):
        self.dss = params['dss']
        self.model = params['model']
        super(DssUnitSensorOperator, self).__init__(*args, **kwargs)

    def poke(self, context):
        try:
            print('-----------------------------------------------------------------------')
            routine_info = context['task_instance'].xcom_pull(task_ids='init_routine')
            print('DssUnitSensorOperator|routine_info : ', routine_info)
            rule_id = routine_info[self.dss]
            print('DssUnitSensorOperator|rule_id : ', rule_id)
            cascade_on = routine_info['cascade_on']
            condition = self.check_completion(rule_id)
            print('DssUnitSensorOperator|condition : ', condition)
            print('-----------------------------------------------------------------------')
            return condition
        except AirflowSensorTimeout as to:
            self._do_skip_downstream_tasks(context)
            raise AirflowSensorTimeout('DssUnitSensorOperator. Time is OUT.')

    def check_completion(self,rule_id):
        completed = False
        print('check_completion|rule_id : ', rule_id)
        db_config = Variable.get('db_config', deserialize_json=True)
        print('check_completion|db_config : ', db_config)
        try:
            adapter = RuleEngineAdapter.get_instance(db_config)
            if self.model == 'wrf':
                print('check_completion|WRF')
                wrf_rule = adapter.get_wrf_rule_status_by_id(rule_id)
                print('check_completion|wrf_rule : ', wrf_rule)
                if wrf_rule is not None:
                    wrf_rule_status = wrf_rule['status']
                    if wrf_rule_status == 3 or wrf_rule_status == '3':
                        completed = True
            elif self.model == 'hechms':
                print('check_completion|HecHms')
                hechms_rule = adapter.get_hechms_rule_status_by_id(rule_id)
                print('check_completion|hechms_rule : ', hechms_rule)
                if hechms_rule is not None:
                    hechms_rule_rule_status = hechms_rule['status']
                    if hechms_rule_rule_status == 3 or hechms_rule_rule_status == '3':
                        completed = True
            elif self.model == 'flo2d':
                print('check_completion|Flo2d')
                flo2d_rule = adapter.get_flo2d_rule_info_by_id(rule_id)
                print('check_completion|flo2d_rule : ', flo2d_rule)
                if flo2d_rule is not None:
                    flo2d_rule_rule_status = flo2d_rule['status']
                    if flo2d_rule_rule_status == 3 or flo2d_rule_rule_status == '3':
                        completed = True
        except Exception as ex:
            print('check_completion|Exception: ', str(ex))
        return completed


class MyFirstPlugin(AirflowPlugin):
    name = "branch_sensor"
    operators = [DssUnitSensorOperator]
