import sys

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/gen_util')
from controller_util import is_matched


def get_next_pump_configurations(dss_adapter, routines):
    dag_info = []
    success_routines = evaluate_configuration_logics(dss_adapter, routines)
    if len(success_routines) > 0:
        for success_routine in success_routines:
            dag_name = success_routine['dag_name']
            payload = success_routine
            dag_info.append({'dag_name': dag_name, 'payload': payload})
    else:
        print('No triggering_pump_dags found.')
    return dag_info


# ((location_name='Yakbedda') and (variable_type='WaterLevel') and ((current_water_level>=alert_water_level) or (current_water_level>=warning_water_level)))
# or
# ((location_name='Kohuwala') and (variable_type='Precipitation') and ((rainfall_intensity>=65.4) or ((last_1_day_rainfall>=150) and (last_3_day_rainfall>=420))))

def evaluate_configuration_logics(dss_adapter, routines):
    print('evaluate_configuration_logics|routines : ', routines)
    passed_routines = []
    for routine in routines:
        rule_logic = routine['rule_logic']
        if is_matched(rule_logic):
            print('evaluate_configuration_logics|rule_logic : ', rule_logic)
            location_names = dss_adapter.evaluate_variable_rule_logic(rule_logic)
            if location_names is not None and len(location_names) > 0:
                passed_routines.append(routine)
    print('evaluate_configuration_logics|passed_routines : ', passed_routines)
    return passed_routines
