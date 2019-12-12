import math
from datetime import datetime, timedelta
import sys
from airflow.models import Variable
import pandas as pd
import numpy as np

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
# sys.path.insert(0, '/home/hasitha/PycharmProjects/DSS-Framework/db_util')
from gen_db import CurwFcstAdapter, CurwObsAdapter, CurwSimAdapter
from dss_db import RuleEngineAdapter

COMMON_DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
STATION_TYPE = 'CUrW_WeatherStation'
MME_TAG = 'MDPA'
VARIABLE_TYPE = 'rainfall'
VARIABLE = 1
UNIT = 1
OBS_VARIABLE = 10
OBS_UNIT = 9
GFS_DAYS = 3


def get_curw_dss_adapter(db_config=None):
    if db_config is None:
        db_config = Variable.get('db_config', deserialize_json=True)
    adapter = RuleEngineAdapter.get_instance(db_config)
    return adapter


def get_curw_fcst_adapter(db_config=None):
    if db_config is None:
        db_config = Variable.get('fcst_db_config', deserialize_json=True)
    adapter = CurwFcstAdapter.get_instance(db_config)
    return adapter


def get_curw_obs_adapter(db_config=None):
    if db_config is None:
        db_config = Variable.get('obs_db_config', deserialize_json=True)
    adapter = CurwObsAdapter.get_instance(db_config)
    return adapter


def get_curw_sim_adapter(db_config=None):
    if db_config is None:
        db_config = Variable.get('sim_db_config', deserialize_json=True)
    adapter = CurwSimAdapter.get_instance(db_config)
    return adapter


def calculate_wrf_rule_accuracy(wrf_rule, exec_datetime):
    print('calculate_wrf_rule_accuracy|wrf_rule : ', wrf_rule)
    print('calculate_wrf_rule_accuracy|execution_date : ', exec_datetime)
    wrf_model = 'WRF_{}'.format(wrf_rule['model'])
    print('calculate_wrf_rule_accuracy|wrf_model : ', wrf_model)
    wrf_version = wrf_rule['version']
    wrf_run = wrf_rule['rule_info']['run']
    wrf_rule_id = wrf_rule['rule_info']['id']
    gfs_hour = wrf_rule['rule_info']['hour']
    accuracy_rule_id = wrf_rule['rule_info']['accuracy_rule']
    sim_tag = 'gfs_d{}_{}'.format(wrf_run, gfs_hour)
    print('calculate_wrf_rule_accuracy|sim_tag : ', sim_tag)
    dss_adapter = get_curw_dss_adapter()
    accuracy_rule = dss_adapter.get_accuracy_rule_info_by_id(accuracy_rule_id)
    print('calculate_wrf_rule_accuracy|accuracy_rule : ', accuracy_rule)
    obs_station_list = format_obs_station_list(accuracy_rule['observed_stations'], accuracy_rule['allowed_error'])
    success_count = 0
    if len(obs_station_list) > 0:
        for [obs_station, allowed_error] in obs_station_list:
            station_error = calculate_station_accuracy(obs_station, wrf_model, wrf_version, wrf_run, gfs_hour,
                                                       exec_datetime, sim_tag)
            if station_error is not None:
                if station_error <= allowed_error:
                    success_count + 1
        total_stations = len(obs_station_list)
        print('calculate_wrf_rule_accuracy|total_stations : ', total_stations)
        print('calculate_wrf_rule_accuracy|success_count : ', success_count)
        accuracy_percentage = (success_count / total_stations) * 100
        print('calculate_wrf_rule_accuracy|accuracy_percentage : ', total_stations)
        dss_adapter.update_wrf_rule_accuracy_level(accuracy_percentage, wrf_rule_id)
        print('wrf rule current accuracy successfully updated.')


def calculate_station_accuracy(obs_station, wrf_model, wrf_version, wrf_run, gfs_hour,
                               exec_datetime, sim_tag, method='MAD'):
    obs_adapter = get_curw_obs_adapter()
    obs_station_id = get_obs_station_id(obs_station, obs_adapter)
    [tms_start, tms_end] = get_wrf_ts_start_end(exec_datetime, wrf_run, gfs_hour)
    tms_start = tms_start.strftime('%Y-%m-%d %H:%M:%S')
    tms_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if obs_station_id is not None:
        obs_hash_id = get_obs_station_hash_id(obs_station_id, obs_adapter)
        obs_df = get_obs_tms(obs_hash_id, exec_datetime, tms_start, tms_end, obs_adapter)
        if obs_df is not None:
            sim_adapter = get_curw_sim_adapter()
            wrf_station_id = get_matching_wrf_station(obs_station, obs_station_id, sim_adapter)
            print('calculate_station_accuracy|wrf_station_id : ', wrf_station_id)
            if wrf_station_id is not None:
                fcst_adapter = get_curw_fcst_adapter()
                wrf_hash_id = get_wrf_station_hash_id(wrf_model, wrf_version, wrf_station_id, exec_datetime, sim_tag,
                                                      fcst_adapter)
                print('calculate_station_accuracy|wrf_hash_id : ', wrf_hash_id)
                if wrf_hash_id is not None:
                    fcst_df = get_fcst_tms(wrf_hash_id, exec_datetime, tms_start, tms_end, fcst_adapter)
                    if fcst_df is not None:
                        print('calculate_station_accuracy|obs_df : ', obs_df)
                        print('calculate_station_accuracy|fcst_df : ', fcst_df)
                        merged_df = obs_df.merge(fcst_df, how='left', on='time')
                        merged_df['cumulative_observed'] = merged_df['observed'].cumsum()
                        merged_df['cumulative_forecast'] = merged_df['forecast'].cumsum()
                        print(merged_df)
                        merged_df['cum_diff'] = merged_df["cumulative_observed"] - merged_df["cumulative_forecast"]
                        row_count = len(merged_df.index)
                        print('row_count : ', row_count)
                        if method == 'MAD':
                            print('MAD')
                            merged_df['abs_cum_diff'] = merged_df['cum_diff'].abs()
                            sum_abs_diff = merged_df['abs_diff'].sum()
                            print('sum_abs_diff : ', sum_abs_diff)
                            mean_absolute_deviation = sum_abs_diff / row_count
                            print('mean_absolute_deviation : ', mean_absolute_deviation)
                            return mean_absolute_deviation
                        elif method == 'RMSE':
                            print('RMSE')
                            merged_df['diff_square'] = np.power((merged_df['cum_diff']), 2)
                            root_mean_square_error = math.sqrt(merged_df['diff_square'].sum() / row_count)
                            print('root_mean_square_error : ', root_mean_square_error)
                            return root_mean_square_error
                        else:
                            print('Invalid method.')
    return None


def format_obs_station_list(obs_stations, allowed_error):
    station_list = obs_stations.split(",")
    print(station_list)
    formatted_list = []
    for station in station_list:
        station_val = station.split('-')
        if len(station_val) == 2:
            formatted_list.append([station_val[0], station_val[1]])
        else:
            formatted_list.append([station_val[0], allowed_error])
    print(formatted_list)
    return formatted_list


def get_obs_station_id(obs_station, obs_adapter=None):
    if obs_adapter is None:
        obs_adapter = get_curw_obs_adapter()
    station_id = obs_adapter.get_station_id_by_name(STATION_TYPE, obs_station)
    if station_id is not None:
        print('get_obs_station_id|station_id : ', station_id)
        return station_id


def get_obs_station_hash_id(obs_station_id, obs_adapter=None):
    if obs_adapter is None:
        obs_adapter = get_curw_obs_adapter()
    hash_id = obs_adapter.get_station_hash_id(obs_station_id, OBS_VARIABLE, OBS_UNIT)
    if hash_id is not None:
        print('get_obs_station_hash_id|hash_id : ', hash_id)
        return hash_id


def get_matching_wrf_station(obs_station, obs_station_id, sim_adapter=None):
    if obs_station_id is not None:
        grid_id = '{}_{}_{}_{}'.format(VARIABLE_TYPE, obs_station_id, obs_station, MME_TAG)
        print('get_matching_wrf_station|grid_id : ', grid_id)
        if sim_adapter is None:
            sim_adapter = get_curw_sim_adapter()
        wrf_station_id = sim_adapter.get_matching_wrf_station_by_grid_id(grid_id)
        if wrf_station_id is not None:
            print('get_matching_wrf_station|wrf_station_id : ', wrf_station_id)
            return wrf_station_id
    return None


def get_wrf_station_hash_id(wrf_model, wrf_version, wrf_station_id, exec_date, sim_tag, fcst_adapter=None):
    if fcst_adapter is None:
        fcst_adapter = get_curw_fcst_adapter()
    source_id = fcst_adapter.get_source_id(wrf_model, wrf_version)
    if source_id is not None:
        print('get_wrf_station_hash_id|source_id : ', source_id)
        hash_id = fcst_adapter.get_hash_id_of_station(VARIABLE, UNIT, source_id, wrf_station_id, sim_tag, exec_date)
        if hash_id is not None:
            print('get_wrf_station_hash_id|hash_id : ', hash_id)
            return hash_id


def get_wrf_ts_start_end(exec_datetime, wrf_run, gfs_hour):
    wrf_run = int(wrf_run)
    exec_datetime = datetime.strptime(exec_datetime, '%Y-%m-%d %H:%M:%S')
    print(exec_datetime)
    exec_date_str = exec_datetime.strftime('%Y-%m-%d')
    exec_date = datetime.strptime(exec_date_str, '%Y-%m-%d')
    print(exec_date)
    ts_start_date = exec_date - timedelta(days=wrf_run)
    ts_start_date_str = ts_start_date.strftime('%Y-%m-%d')
    print(ts_start_date_str)
    gfs_ts_start_utc_str = '{} {}:00:00'.format(ts_start_date_str, gfs_hour)
    print(gfs_ts_start_utc_str)
    gfs_ts_start_utc = datetime.strptime(gfs_ts_start_utc_str, '%Y-%m-%d %H:%M:%S')
    gfs_ts_start_local = gfs_ts_start_utc + timedelta(hours=5, minutes=30)
    gfs_ts_end_local = gfs_ts_start_local + timedelta(days=GFS_DAYS)
    return [gfs_ts_start_local, gfs_ts_end_local]


def get_fcst_tms(wrf_station_hash_id, exec_datetime, tms_start, tms_end, fcst_adapter=None):
    if fcst_adapter is None:
        fcst_adapter = get_curw_fcst_adapter()
    tms_df = fcst_adapter.get_wrf_station_tms(wrf_station_hash_id, exec_datetime, tms_start, tms_end)
    if tms_df is not None:
        return format_df_to_time_indexing(tms_df)


def format_df_to_time_indexing(tms_df):
    tms_df['time'] = pd.to_datetime(tms_df['time'], format=COMMON_DATE_TIME_FORMAT)
    print('format_df_to_time_indexing|tms_df : ', tms_df)
    tms_df.set_index('time', inplace=True)
    return tms_df


def get_obs_tms(obs_station_hash_id, exec_datetime, tms_start, tms_end, obs_adapter=None):
    if obs_adapter is None:
        obs_adapter = get_curw_obs_adapter()
    tms_df = obs_adapter.get_timeseries_by_id(obs_station_hash_id, tms_start, tms_end)
    if tms_df is not None:
        return format_df_to_15min_intervals(tms_df)


def format_df_to_15min_intervals(tms_df):
    tms_df = format_df_to_time_indexing(tms_df)
    min15_ts = pd.DataFrame()
    min15_ts['value'] = tms_df['value'].resample('15min', label='right', closed='right').sum()
    print(min15_ts)
    return min15_ts


if __name__ == "__main__":
    # obs_db_config = {'mysql_user': 'admin', 'mysql_password': 'floody', 'mysql_host': '35.227.163.211',
    #                  'mysql_db': 'curw_obs', 'log_path': '/home/hasitha/PycharmProjects/DSS-Framework/log'}
    # print(len(obs_db_config.keys()))
    # sim_db_config = {'mysql_user': 'admin', 'mysql_password': 'floody', 'mysql_host': '35.227.163.211',
    #                  'mysql_db': 'curw_sim', 'log_path': '/home/hasitha/PycharmProjects/DSS-Framework/log'}
    # fcst_db_config = {'mysql_user': 'admin', 'mysql_password': 'floody', 'mysql_host': '35.227.163.211',
    #                   'mysql_db': 'curw_fcst', 'log_path': '/home/hasitha/PycharmProjects/DSS-Framework/log'}
    # obs_adapter = get_curw_obs_adapter(obs_db_config)
    # sim_adapter = get_curw_sim_adapter(sim_db_config)
    # fcst_adapter = get_curw_fcst_adapter(fcst_db_config)
    # print(get_matching_wrf_station('Arangala', obs_adapter, sim_adapter))
    print(get_wrf_ts_start_end('2019-12-07 07:21:32', '2', '12'))
