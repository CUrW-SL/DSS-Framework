import json
import glob


class RuleStore:
    def __init__(self, rule_dir_path):
        self.rule_dir_path = rule_dir_path
        self.all_rule_data = self.load_all_rules()

    def load_all_rules(self):
        all_rule_data = {}

        def _load_rule(rule_file_path):
            with open(rule_file_path) as f:
                data = json.load(f)
                return data
        rule_file_list = glob.glob("{}/*.{}".format(self.rule_dir_path, 'json'))
        if rule_file_list:
            for rule_file in rule_file_list:
                rule_data = _load_rule(rule_file)
                all_rule_data[rule_data['rule_id']] = rule_data
        else:
            print('No defined rules.')
        return all_rule_data

    def view_all_rules(self):
        for rule_id in self.all_rule_data:
            print(self.all_rule_data[rule_id])


def execute_exec_rule(rule_store, rule_id):
    rule_data = rule_store.all_rule_data[rule_id]
    print('rule_data : ', rule_data)
    if rule_data['rule_type'] == 'exec' and rule_data['enable'] == 'true':

        print('')
    else:
        print('Rule {} is disabled.'.format(rule_id))


def execute_wrf_model_rules():
    print('')


def execute_hechms_model_rules():
    print('')


def execute_flo2d_model_rules():
    print('')


def execute_accuracy_rule():
    print()


def execute_alert_rule():
    print()


def execute_alert_rule():
    print()


rule_store = RuleStore('/home/hasitha/PycharmProjects/DSS-Framework/rule_engine/rule_scripts')
execute_exec_rule(rule_store, 'rule0')

