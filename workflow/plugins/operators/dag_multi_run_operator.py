"""
This operator can create multiple runs of same dag.
"""
from datetime import datetime

from airflow.models import DagBag
from airflow.utils.state import State

from airflow import settings
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.utils import apply_defaults


class TriggerMultiDagRunOperator(TriggerDagRunOperator):
    @apply_defaults
    def __init__(self, op_args=None, op_kwargs=None, *args, **kwargs):
        super(TriggerMultiDagRunOperator, self).__init__(*args, **kwargs)
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

    def execute(self, context):
        session = settings.Session()
        created = False
        for dro in self.python_callable(context, *self.op_args, **self.op_kwargs):
            if not dro or not isinstance(dro, DagRunOrder):
                break

            if dro.run_id is None:
                dro.run_id = 'trig__' + datetime.utcnow().isoformat()

            dbag = DagBag(settings.DAGS_FOLDER)
            trigger_dag = dbag.get_dag(self.trigger_dag_id)
            dr = trigger_dag.create_dagrun(
                run_id=dro.run_id,
                state=State.RUNNING,
                conf=dro.payload,
                external_trigger=True
            )
            created = True
            self.log.info("Creating DagRun %s", dr)

        if created is True:
            session.commit()
        else:
            self.log.info("No DagRun created")
        session.close()

