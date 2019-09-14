"""
This operator can create multiple dag runs.
"""
import datetime
import six
from workflow.bin.cli import trigger_dag
from workflow.models import BaseOperator
from workflow.utils import timezone, json
from workflow.utils.decorators import apply_defaults


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class TriggerMultiDagRunOperator(BaseOperator):
    """
    :param trigger_dag_ids: dag_ids to trigger (templated)
    :type trigger_dag_ids: list
    :type python_callable: python callable
    :param python_callable: return True of False
    """
    @apply_defaults
    def __init__(
            self,
            trigger_dag_ids,
            python_callable=None,
            execution_date=None,
            *args, **kwargs):
        super(TriggerMultiDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.trigger_dag_ids = trigger_dag_ids

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
        if self.trigger_dag_ids is not None:
            if self.python_callable is not None:
                if self.python_callable(context):
                    for trigger_dag_id in self.trigger_dag_ids:
                        if self.execution_date is not None:
                            run_id = 'trig__{}'.format(self.execution_date)
                            self.execution_date = timezone.parse(self.execution_date)
                        else:
                            run_id = 'trig__' + timezone.utcnow().isoformat()
                        dro = DagRunOrder(run_id=run_id)
                        trigger_dag(dag_id=trigger_dag_id,
                                    run_id=dro.run_id,
                                    conf=json.dumps(dro.payload),
                                    execution_date=self.execution_date,
                                    replace_microseconds=False)
                else:
                    self.log.info("Triggering Condition failed, moving on")
        else:
            self.log.info("Criteria not met, moving on")
