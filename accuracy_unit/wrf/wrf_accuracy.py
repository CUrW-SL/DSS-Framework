from datetime import datetime, timedelta
import sys
from airflow.models import Variable

# sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
sys.path.insert(0, '/home/hasitha/PycharmProjects/DSS-Framework/db_util')
from gen_db import CurwFcstAdapter, CurwObsAdapter, CurwSimAdapter

STATION_TYPE = 'CUrW_WeatherStation'
MME_TAG = 'MDPA'
VARIABLE_TYPE = 'rainfall'
VARIABLE = 1
UNIT = 1
GFS_DAYS = 3


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


def get_matching_wrf_station(obs_station, obs_adapter=None, sim_adapter=None):
    if obs_adapter is None:
        obs_adapter = get_curw_obs_adapter()
    station_id = obs_adapter.get_station_id_by_name(STATION_TYPE, obs_station)
    if station_id is not None:
        grid_id = '{}_{}_{}_{}'.format(VARIABLE_TYPE, station_id, obs_station, MME_TAG)
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
        hash_id = fcst_adapter.get_hash_id_of_wrf_station(VARIABLE, UNIT, source_id, wrf_station_id, sim_tag, exec_date)
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
