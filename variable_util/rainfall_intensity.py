import sys
from datetime import datetime
import pandas as pd

DEFAULT_VARIABLE_VALUE = -9999
TIME_STEPS_FOR_HOUR = 12

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/variable_util')
from common_util import get_time_gap_of_two_times, search_in_dictionary_list, lower_time_limit


def update_rainfall_intensity_values(dss_adapter, obs_adapter, variable_routine):
    print('update_rainfall_intensity_values|variable_routine : ', variable_routine)
    locations = dss_adapter.get_location_names_from_rule_variables(variable_routine['variable_type'])
    variable_values = obs_adapter.get_rainfall_for_given_location_set(locations,
                                                                      variable_routine['variable_type'])
    print('update_current_rainfall_values|variable_values : ', variable_values)
    current_time = datetime.now()
    print('update_rainfall_intensity_values|current_time : ', current_time)
    for variable_value in variable_values:
        rainfall_values = pd.DataFrame(data=variable_value['results'], columns=['time', 'value'])
        print('update_current_rainfall_values|rainfall_values : ', rainfall_values)
        validate_rainfall_values(rainfall_values)


def validate_rainfall_values(rainfall_values):
    row_count = rainfall_values.shape()[0]
    if row_count == TIME_STEPS_FOR_HOUR:
        start_time = rainfall_values['time'].iloc[0]
        print('validate_rainfall_values|start_time : ', start_time)
        end_time = rainfall_values['time'].iloc[-1]
        time_gap = get_time_gap_of_two_times(end_time, start_time)
        print('validate_rainfall_values|time_gap : ', time_gap)
    else:
        return False


def validate_variable_value(variable_time, current_time, cron_exp):
    lower_limit = lower_time_limit(current_time, cron_exp)
    if variable_time > lower_limit:
        return True
    else:
        return False
