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
                print('load_rule_json|data : ', data)
                return data
        rule_file_list = glob.glob("{}/*.{}".format(self.rule_dir_path, 'json'))
        if rule_file_list:
            for rule_file in rule_file_list:
                rule_data = _load_rule(rule_file)
                all_rule_data[rule_data['rule_id']] = rule_data
        else:
            print('No defined rules.')
        return all_rule_data


def view_all_rules(rule_store):
    for rule_id in rule_store:
        print(rule_store[rule_id])


def execute_exec_rule(rule_store, rule_id):
    rule_data = rule_store.all_rule_data[rule_id]
    print('rule_data : ', rule_data)


def execute_accuracy_rule():
    print()


def execute_alert_rule():
    print()


def execute_alert_rule():
    print()


rule_store = RuleStore('/home/hasitha/PycharmProjects/DSS-Framework/rule_engine/rule_scripts')
execute_exec_rule(rule_store, 'rule0')

