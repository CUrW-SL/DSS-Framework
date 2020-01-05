import json
from datetime import datetime, timezone
import six
from airflow.bin.cli import trigger_dag
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.api.common.experimental.trigger_dag import trigger_dag


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class DynamicTriggerDagRunOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            python_callable=None,
            execution_date=None,
            *args, **kwargs):
        super(DynamicTriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable

        if isinstance(execution_date, datetime):
            self.execution_date = execution_date.isoformat()
        elif isinstance(execution_date, six.string_types):
            self.execution_date = execution_date
        elif execution_date is None:
            self.execution_date = execution_date
        else:
            raise TypeError(
                'Expected str or datetime.datetime type '
                'for execution_date. Got {}'.format(
                    type(execution_date)))

    def execute(self, context):
        if self.python_callable is not None:
            results = self.python_callable(context)
            print('DynamicTriggerDagRunOperator|execute|results : ', results)
            count = 1
            for result in results:
                print('result : ', result)
                if self.execution_date is not None:
                    run_id = 'trig_{}_{}'.format(count, self.execution_date)
                    self.execution_date = timezone.parse(self.execution_date)
                else:
                    run_id = 'trig_{}_{}'.format(count, timezone.utcnow().isoformat())
                dro = DagRunOrder(run_id=run_id)
                trigger_dag_id = result['dag_name']
                print('trigger_dag_id : ', trigger_dag_id)
                payload = result['payload']
                print('payload : ', payload)
                trigger_dag(dag_id=trigger_dag_id,
                            run_id=dro.run_id,
                            conf=json.dumps(payload),
                            execution_date=self.execution_date,
                            replace_microseconds=False)
                count += 1
        else:
            print('DynamicTriggerDagRunOperator|python_callable is None.')
            self.log.info("Criteria not met, moving on")


class MyFirstPlugin(AirflowPlugin):
    name = "dynamic_dag_run_operator"
    operators = [DynamicTriggerDagRunOperator]
