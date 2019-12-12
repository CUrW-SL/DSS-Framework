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
STATION_TYPE = 'CUrW_WaterLevelGauge'
MME_TAG = 'MDPA'
VARIABLE_TYPE = 'waterlevel'
VARIABLE = 2
UNIT = 2
OBS_VARIABLE = 17
OBS_UNIT = 15


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


def calculate_flo2d_rule_accuracy(flo2d_rule, exec_datetime):
    print('calculate_flo2d_rule_accuracy|flo2d_rule : ', flo2d_rule)
    print('calculate_flo2d_rule_accuracy|execution_date : ', exec_datetime)
    flo2d_model = flo2d_rule['model']
    flo2d_version = flo2d_rule['version']
    print('calculate_flo2d_rule_accuracy|flo2d_model : ', flo2d_model)
    flo2d_rule_id = flo2d_rule['rule_info']['id']
    accuracy_rule_id = flo2d_rule['rule_info']['accuracy_rule']
    observed_days = flo2d_rule['rule_info']['observed_days']
    print('calculate_flo2d_rule_accuracy|observed_days : ', observed_days)
    sim_tag = 'hourly_run'
    print('calculate_flo2d_rule_accuracy|sim_tag : ', sim_tag)
    dss_adapter = get_curw_dss_adapter()
    accuracy_rule = dss_adapter.get_accuracy_rule_info_by_id(accuracy_rule_id)
    print('calculate_flo2d_rule_accuracy|accuracy_rule : ', accuracy_rule)
    obs_station_list = format_obs_station_list(accuracy_rule['observed_stations'], accuracy_rule['allowed_error'])
    station_result = {}
    success_count = 0
    if len(obs_station_list) > 0:
        for [obs_station, allowed_error] in obs_station_list:
            station_error = calculate_station_accuracy(obs_station, flo2d_model, flo2d_version,
                                                       exec_datetime, observed_days, sim_tag)
            if station_error is not None:
                if station_error <= allowed_error:
                    station_result[obs_station] = True
                    success_count + 1
                else:
                    station_result[obs_station] = False
            else:
                station_result[obs_station] = False
        total_stations = len(station_result.keys())
        print('calculate_flo2d_rule_accuracy|total_stations : ', total_stations)
        print('calculate_flo2d_rule_accuracy|success_count : ', success_count)
        accuracy_percentage = (success_count / total_stations) * 100
        print('calculate_flo2d_rule_accuracy|accuracy_percentage : ', accuracy_percentage)
        dss_adapter.update_flo2d_rule_accuracy_level(accuracy_percentage, flo2d_rule_id)
        print('flo2d rule current accuracy successfully updated.')


def calculate_station_accuracy(obs_station, flo2d_model, flo2d_version,
                               exec_datetime, observed_days, sim_tag, method='MAD'):
    obs_adapter = get_curw_obs_adapter()
    obs_station_id = get_obs_station_id(obs_station, obs_adapter, STATION_TYPE)
    [tms_start, tms_end] = get_flo2d_ts_start_end(exec_datetime, observed_days)
    tms_start = tms_start.strftime('%Y-%m-%d %H:%M:%S')
    tms_end = tms_end.strftime('%Y-%m-%d %H:%M:%S')
    print('calculate_station_accuracy|[tms_start, tms_end] : ', [tms_start, tms_end])
    if obs_station_id is not None:
        obs_hash_id = get_obs_station_hash_id(obs_station_id, obs_adapter)
        obs_df = get_obs_tms(obs_hash_id, exec_datetime, tms_start, tms_end, obs_adapter)
        if obs_df is not None:
            fcst_adapter = get_curw_fcst_adapter()
            cell_id = get_cell_id(obs_station, flo2d_model, flo2d_version)
            if cell_id is not None:
                flo2d_station_id = get_matching_flo2d_station(obs_station, cell_id, fcst_adapter)
                print('calculate_station_accuracy|flo2d_station_id : ', flo2d_station_id)
                if flo2d_station_id is not None:
                    fcst_adapter = get_curw_fcst_adapter()
                    flo2d_hash_id = get_flo2d_station_hash_id(flo2d_model, flo2d_version, flo2d_station_id,
                                                              exec_datetime,
                                                              sim_tag, fcst_adapter)
                    print('calculate_station_accuracy|flo2d_hash_id : ', flo2d_hash_id)
                    if flo2d_hash_id is not None:
                        fcst_df = get_fcst_tms(flo2d_hash_id, exec_datetime, tms_start, tms_end, fcst_adapter)
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


def get_cell_id(station_name, model, version, fcst_adapter=None):
    if fcst_adapter is None:
        fcst_adapter = get_curw_fcst_adapter()
    cell_map = fcst_adapter.get_flo2d_cell_map(model, version)
    if cell_map is not None:
        channel_cell_map = cell_map['CHANNEL_CELL_MAP']
        flood_plain_cell_map = cell_map['FLOOD_PLAIN_CELL_MAP']
        if station_name in channel_cell_map:
            cell_id = channel_cell_map[station_name]
        elif station_name in flood_plain_cell_map:
            cell_id = channel_cell_map[station_name]
        return cell_id
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


def get_obs_station_id(obs_station, obs_adapter=None, station_type=STATION_TYPE):
    if obs_adapter is None:
        obs_adapter = get_curw_obs_adapter()
    station_id = obs_adapter.get_station_id_by_name(station_type, obs_station)
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


def get_matching_flo2d_station(obs_station, cell_id, fcst_adapter=None):
    fcst_station_name = '{}_{}'.format(cell_id, obs_station)
    if fcst_adapter is None:
        fcst_adapter = get_curw_fcst_adapter()
    flo2d_station_id = fcst_adapter.get_flo2d_station_id_by_name(fcst_station_name)
    if flo2d_station_id is not None:
        print('get_matching_flo2d_station|flo2d_station_id : ', flo2d_station_id)
        return flo2d_station_id


def get_flo2d_station_hash_id(flo2d_model, flo2d_version, flo2d_station_id, exec_date, sim_tag, fcst_adapter=None):
    if fcst_adapter is None:
        fcst_adapter = get_curw_fcst_adapter()
    source_id = fcst_adapter.get_source_id(flo2d_model, flo2d_version)
    if source_id is not None:
        print('get_flo2d_station_hash_id|source_id : ', source_id)
        hash_id = fcst_adapter.get_hash_id_of_station(VARIABLE, UNIT, source_id, flo2d_station_id, sim_tag,
                                                      exec_date)
        if hash_id is not None:
            print('get_flo2d_station_hash_id|hash_id : ', hash_id)
            return hash_id


def get_flo2d_ts_start_end(exec_datetime, observed_days):
    observed_days = int(observed_days)
    exec_datetime = datetime.strptime(exec_datetime, '%Y-%m-%d %H:%M:%S')
    print(exec_datetime)
    exec_date_str = exec_datetime.strftime('%Y-%m-%d')
    exec_date = datetime.strptime(exec_date_str, '%Y-%m-%d')
    print(exec_date)
    ts_start_date = exec_date - timedelta(days=observed_days)
    ts_start_date_str = ts_start_date.strftime('%Y-%m-%d')
    print(ts_start_date_str)
    gfs_ts_start_utc_str = '{} 00:00:00'.format(ts_start_date_str)
    print(gfs_ts_start_utc_str)
    gfs_ts_start_utc = datetime.strptime(gfs_ts_start_utc_str, '%Y-%m-%d %H:%M:%S')
    return [gfs_ts_start_utc, exec_datetime]


def get_fcst_tms(flo2d_station_hash_id, exec_datetime, tms_start, tms_end, fcst_adapter=None):
    if fcst_adapter is None:
        fcst_adapter = get_curw_fcst_adapter()
    tms_df = fcst_adapter.get_wrf_station_tms(flo2d_station_hash_id, exec_datetime, tms_start, tms_end)
    if tms_df is not None:
        return format_df_to_time_indexing(tms_df)


def format_df_to_time_indexing(tms_df):
    tms_df['time'] = pd.to_datetime(tms_df['time'], format=COMMON_DATE_TIME_FORMAT)
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
    fcst_db_config = {'mysql_user': 'admin', 'mysql_password': 'floody', 'mysql_host': '35.227.163.211',
                      'mysql_db': 'curw_fcst', 'log_path': '/home/hasitha/PycharmProjects/DSS-Framework/log'}
    # obs_adapter = get_curw_obs_adapter(obs_db_config)
    # sim_adapter = get_curw_sim_adapter(sim_db_config)
    fcst_adapter = get_curw_fcst_adapter(fcst_db_config)
    # print(get_matching_flo2d_station('Arangala', obs_adapter, sim_adapter))
    print(fcst_adapter.get_flo2d_cell_map('FLO2D', 250))
