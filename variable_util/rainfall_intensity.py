import sys
from datetime import datetime
import pandas as pd

DEFAULT_VARIABLE_VALUE = -9999

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/variable_util')
from common_util import get_time_gap_of_two_times, search_in_dictionary_list, lower_time_limit


def update_rainfall_intensity_values(dss_adapter, obs_adapter, variable_routine):
    print('update_rainfall_intensity_values|variable_routine : ', variable_routine)
    locations = dss_adapter.get_location_names_from_rule_variables(variable_routine['variable_type'])
    variable_values = obs_adapter.get_rainfall_for_given_location_set(locations,
                                                                      variable_routine['variable_type'])
    print('update_current_rainfall_values|variable_values : ', variable_values)
    current_time = datetime.now()
    for variable_value in variable_values:
        rainfall_values = pd.DataFrame(variable_value['results'], columns=['time', 'value'])
        print('update_current_rainfall_values|rainfall_values : ', rainfall_values)


def validate_rainfall_values(rainfall_values):
    if len(rainfall_values) == 12:
        start_time = rainfall_values[0][0]
        end_time = rainfall_values[11][0]
        time_gap = get_time_gap_of_two_times(end_time, start_time)
        if {'days': 0, 'hours': 1, 'minutes': 0, 'seconds': 0} == time_gap:
            print('')
    else:
        return False


def validate_variable_value(variable_time, current_time, cron_exp):
    lower_limit = lower_time_limit(current_time, cron_exp)
    if variable_time > lower_limit:
        return True
    else:
        return False
