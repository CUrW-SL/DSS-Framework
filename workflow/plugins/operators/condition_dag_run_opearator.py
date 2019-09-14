import datetime
import six
from workflow.models import BaseOperator
from workflow.plugins_manager import AirflowPlugin
from workflow.utils import timezone
from workflow.utils.decorators import apply_defaults
from workflow.api.common.experimental.trigger_dag import trigger_dag

import json


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class ConditionTriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id`` return by python_callable
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
        return dag (templated) which match with given conditions {trigger_dag_id:"trigger_dag_id", dro:"dro"}
    :type python_callable: python callable
    :type execution_date: str or datetime.datetime
    """

    @apply_defaults
    def __init__(
            self,
            default_trigger,
            python_callable=None,
            execution_date=None,
            *args, **kwargs):
        super(ConditionTriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.default_trigger = default_trigger

        if isinstance(execution_date, datetime.datetime):
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
        if self.execution_date is not None:
            run_id = 'trig__{}'.format(self.execution_date)
            self.execution_date = timezone.parse(self.execution_date)
        else:
            run_id = 'trig__' + timezone.utcnow().isoformat()
        dro = DagRunOrder(run_id=run_id)
        if self.python_callable is not None:
            condition_result = self.python_callable(context, dro)
            dro = condition_result['dro']
            trigger_dag_id = condition_result['trigger_dag_id']
            trigger_dag(dag_id=trigger_dag_id,
                        run_id=dro.run_id,
                        conf=json.dumps(dro.payload),
                        execution_date=self.execution_date,
                        replace_microseconds=False)
        else:
            if self.default_trigger is not None:
                trigger_dag(dag_id=self.default_trigger,
                            run_id=dro.run_id,
                            execution_date=self.execution_date,
                            replace_microseconds=False)
            else:
                self.log.info("Criteria not met, moving on")


class MyFirstPlugin(AirflowPlugin):
    name = "conditional_trigger_operator"
    operators = [ConditionTriggerDagRunOperator]

