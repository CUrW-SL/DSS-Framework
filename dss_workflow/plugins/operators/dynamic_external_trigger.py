from datetime import datetime
from airflow import settings
from airflow.models import DagBag
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class DynamicTriggerDagRunOperator(TriggerDagRunOperator):
    @apply_defaults
    def __init__(self, op_args=None, op_kwargs=None, *args, **kwargs):
        super(DynamicTriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

    def execute(self, context):
        session = settings.Session()
        created = False
        dro = self.python_callable(context, *self.op_args, **self.op_kwargs)
        if dro or isinstance(dro, DagRunOrder):
            if dro.run_id is None:
                dro.run_id = 'trig__' + datetime.utcnow().isoformat()
            print('DynamicTriggerDagRunOperator|dro : ', dro)
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


class MyFirstPlugin(AirflowPlugin):
    name = "dynamic_dag_run_operator"
    operators = [DynamicTriggerDagRunOperator]
