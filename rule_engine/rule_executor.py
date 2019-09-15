from database.db_layer.mysql_adapter import RuleEngineAdapter


class RuleEngineExecutor:
    def __init__(self, db_config=None):
        self.db_adapter = RuleEngineAdapter(db_config['mysql_user'],
                                            db_config['mysql_password'],
                                            db_config['mysql_host'],
                                            db_config['mysql_db'],
                                            db_config['log_path'])

    def execute_wrf_model_rules(self):
        dag_name = ''
        print('')
        return dag_name

    def execute_hechms_model_rules(self):
        print('')

    def execute_flo2d_model_rules(self):
        print('')

    def execute_accuracy_rules(self):
        print()

    def execute_variable_rules(self):
        print()


