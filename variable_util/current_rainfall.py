import sys
from datetime import datetime, timedelta

DEFAULT_VARIABLE_VALUE = -9999

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/variable_util')
from common_util import get_iteration_gap_of_cron_exp, search_in_dictionary_list


def update_current_rainfall_values(dss_adapter, obs_adapter, variable_routine):
    print('update_current_rainfall_values|variable_routine : ', variable_routine)
    locations = dss_adapter.get_location_names_from_rule_variables(variable_routine['variable_type'])
    variable_values = obs_adapter.get_current_rainfall_for_given_location_set(locations,
                                                                              variable_routine['variable_type'])
    print('update_current_rainfall_values|variable_values : ', variable_values)
    if variable_values is not None:
        for location in locations:
            variable_value_rec = search_in_dictionary_list(variable_values, location)
            if variable_value_rec is not None:
                variable_time = datetime.strptime('', '%Y-%m-%d %H:%M:%S')
                current_time = datetime.now()
                if validate_variable_value(variable_time, current_time, variable_routine['schedule']):
                    variable_value = variable_value_rec['value']
                    print('update_current_rainfall_values|variable_value : ', variable_value)
            else:
                variable_value = DEFAULT_VARIABLE_VALUE
            print('update_current_rainfall_values|variable_value : ', variable_value)
            print('update_current_rainfall_values|variable_type : ', variable_routine['variable_type'])
            print('update_current_rainfall_values|location : ', location)
            dss_adapter.update_variable_value(variable_value, variable_routine['variable_type'], location)


def validate_variable_value(variable_time, current_time, cron_exp):
    lower_limit = lower_time_limit(current_time, cron_exp)
    if variable_time > lower_limit:
        return True
    else:
        return False


def lower_time_limit(current_time, cron_exp):
    time_gap = get_iteration_gap_of_cron_exp(cron_exp)
    lower_limit = current_time - timedelta(days=time_gap['days'],
                                           hours=time_gap['hours'],
                                           minutes=time_gap['minutes'],
                                           seconds=time_gap['seconds'])
    print('lower_time_limit|lower_limit : ', lower_limit)
    return lower_limit
