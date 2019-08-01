import json


def load_rule_json(rule_file_path):
    with open(rule_file_path) as f:
        data = json.load(f)
        print('load_rule_json|data : ', data)
        print('load_rule_json|data : ', type(data))
        return data


def execute_exec_rule(rule_id):
    print()


def execute_accuracy_rule():
    print()


def execute_alert_rule():
    print()


def execute_alert_rule():
    print()


load_rule_json('/home/hasitha/PycharmProjects/DSS-Framework/rule_engine/rule_scripts/rule0.json')

