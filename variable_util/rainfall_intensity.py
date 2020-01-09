import sys
from datetime import datetime

DEFAULT_VARIABLE_VALUE = -9999

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/variable_util')
from common_util import get_iteration_gap_of_cron_exp, search_in_dictionary_list, lower_time_limit


def update_rainfall_intensity_values(dss_adapter, obs_adapter, variable_routine):
    print('update_rainfall_intensity_values|variable_routine : ', variable_routine)
    locations = dss_adapter.get_location_names_from_rule_variables(variable_routine['variable_type'])
    variable_values = obs_adapter.get_rainfall_for_given_location_set(locations,
                                                                      variable_routine['variable_type'])
    print('update_current_rainfall_values|variable_values : ', variable_values)


def validate_variable_value(variable_time, current_time, cron_exp):
    lower_limit = lower_time_limit(current_time, cron_exp)
    if variable_time > lower_limit:
        return True
    else:
        return False
