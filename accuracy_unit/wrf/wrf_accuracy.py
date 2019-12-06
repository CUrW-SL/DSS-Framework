from datetime import datetime
import sys
from airflow.models import Variable

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
from gen_db import CurwFcstAdapter, CurwObsAdapter, CurwSimAdapter

STATION_TYPE = 'CUrW_WeatherStation'
MME_TAG = 'MDPA'
VARIABLE_TYPE = 'rainfall'


# {
# 'model': 'E', 'version': '4.0',
# 'rule_info': {'id': 3, 'run': '1', 'hour': '00', 'ignore_previous_run': 1, 'check_gfs_data_availability': 1}
# }


def get_curw_fcst_adapter():
    db_config = Variable.get('fcst_db_config', deserialize_json=True)
    adapter = CurwFcstAdapter.get_instance(db_config)
    return adapter


def get_curw_obs_adapter():
    db_config = Variable.get('obs_db_config', deserialize_json=True)
    adapter = CurwFcstAdapter.get_instance(db_config)
    return adapter


def get_curw_sim_adapter():
    db_config = Variable.get('sim_db_config', deserialize_json=True)
    adapter = CurwFcstAdapter.get_instance(db_config)
    return adapter


def calculate_wrf_rule_accuracy(wrf_rule, execution_date):
    print('calculate_wrf_rule_accuracy|wrf_rule : ', wrf_rule)
    print('calculate_wrf_rule_accuracy|execution_date : ', execution_date)
    wrf_model = 'WRF_{}'.format(wrf_rule['model'])
    print('calculate_wrf_rule_accuracy|wrf_model : ', wrf_model)
    wrf_version = wrf_rule['version']
    wrf_run = wrf_rule['rule_info']['run']
    gfs_hour = wrf_rule['rule_info']['hour']
    accuracy_rule = wrf_rule['rule_info']['accuracy_rule']
    sim_tag = 'gfs_d{}_{}'.format(wrf_run, gfs_hour)
    print('calculate_wrf_rule_accuracy|sim_tag : ', sim_tag)


def get_matching_wrf_station(obs_station):
    obs_adapter = get_curw_obs_adapter()
    station_id = obs_adapter.get_station_id_by_name(STATION_TYPE, obs_station)
    if station_id is not None:
        grid_id = '{}_{}_{}_{}'.format(VARIABLE_TYPE, station_id, obs_station, MME_TAG)
        print('get_matching_wrf_station|grid_id : ', grid_id)
        sim_adapter = get_curw_sim_adapter()
        wrf_station_id = sim_adapter.get_matching_wrf_station_by_grid_id(grid_id)
        if wrf_station_id is not None:
            print('get_matching_wrf_station|wrf_station_id : ', wrf_station_id)
            return wrf_station_id
    return None
