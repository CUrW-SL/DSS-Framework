from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
import sys

sys.path.insert(0, '/home/curw/git/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

sys.path.insert(0, '/home/curw/git/DSS-Framework/gen_util')
from dynamic_dag_util import get_all_dynamic_dag_routines, get_dynamic_dag_tasks, get_trigger_target_dag


def get_dss_db_adapter():
    adapter = None
    try:
        db_config = Variable.get('db_config', deserialize_json=True)
        try:
            adapter = RuleEngineAdapter.get_instance(db_config)
        except Exception as ex:
            print('get_dss_db_adapter|Exception: ', str(ex))
    except Exception as e:
        print('get_dss_db_adapter|db_config|Exception: ', str(e))
    return adapter


def get_model_rule_status_from_context(dss_adapter, context):
    condition = False
    task_name = context['task'].task_id
    dag_rule_id = context['params']['id']
    print('get_model_rule_status_from_context|[task_name, dag_rule_id] : ', [task_name, dag_rule_id])
    target_dag_info = get_trigger_target_dag(dss_adapter, dag_rule_id, task_name)
    print('get_model_rule_status_from_context|target_dag_info : ', target_dag_info)
    model_type = target_dag_info['input_params']['model_type']
    model_rule = target_dag_info['input_params']['rule_id']
    print('get_model_rule_status_from_context|[model_type, model_rule] : ', [model_type, model_rule])
    if model_type == 'wrf':
        rule_status = dss_adapter.get_wrf_rule_status_by_id(model_rule)
    elif model_type == 'hechms':
        rule_status = dss_adapter.get_hechms_rule_status_by_id(model_rule)
    elif model_type == 'flo2d':
        rule_status = dss_adapter.get_flo2d_rule_status_by_id(model_rule)
    else:
        print('get_model_rule_status_from_context|payload is must.')

    if rule_status is not None:
        if rule_status['status'] == 3:
            condition = True
    return condition


class ExternalDagSensorOperator(BaseSensorOperator):
    @apply_defaults
    def __init__(self, model_type=None, model_rule_id=None, *args, **kwargs):
        self.model_type = model_type
        self.model_rule_id = model_rule_id
        super(ExternalDagSensorOperator, self).__init__(*args, **kwargs)

    def poke(self, context, session=None):
        condition = False
        try:
            print('-----------------------------------------------------------------------')
            dss_adapter = get_dss_db_adapter()
            if dss_adapter is not None:
                print('ExternalDagSensorOperator|[model_type, model_rule_id]:', [self.model_type, self.model_rule_id])
                if self.model_type and self.model_rule_id:
                    if self.model_type == 'wrf':
                        rule_status = dss_adapter.get_wrf_rule_status_by_id(self.model_rule_id)
                    elif self.model_type == 'hechms':
                        rule_status = dss_adapter.get_hechms_rule_status_by_id(self.model_rule_id)
                    elif self.model_type == 'flo2d':
                        rule_status = dss_adapter.get_flo2d_rule_status_by_id(self.model_rule_id)
                    else:
                        print('ExternalDagSensorOperator|payload is must.')
                    print('ExternalDagSensorOperator|rule_status : ', rule_status)
                    if rule_status is not None:
                        if rule_status['status'] == 3:
                            condition = True
                else:
                    condition = get_model_rule_status_from_context(dss_adapter, context)
            print('ExternalDagSensorOperator|condition : ', condition)
            print('-----------------------------------------------------------------------')
            return condition
        except AirflowSensorTimeout as to:
            self._do_skip_downstream_tasks(context)
            raise AirflowSensorTimeout('ExternalDagSensorOperator. Time is OUT.')
        finally:
            print('ExternalDagSensorOperator|condition : ', condition)
            return condition


class MyFirstPlugin(AirflowPlugin):
    name = "ExternalDagSensorOperator"
    operators = [ExternalDagSensorOperator]
