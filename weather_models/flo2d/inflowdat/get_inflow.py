#!"D:\inflow\venv\Scripts\python.exe"
import pymysql
from datetime import datetime, timedelta
import traceback
import json
import os
import sys
import getopt

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_HOST, CURW_SIM_PASSWORD, CURW_SIM_PORT, CURW_SIM_USERNAME
from db_adapter.constants import CURW_OBS_DATABASE, CURW_OBS_PORT, CURW_OBS_PASSWORD, CURW_OBS_USERNAME, CURW_OBS_HOST
from db_adapter.curw_sim.timeseries.discharge import Timeseries as DisTS
from db_adapter.constants import COMMON_DATE_TIME_FORMAT


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def append_to_file(file_name, data):
    with open(file_name, 'a+') as f:
        f.write('\n'.join(data))


def read_attribute_from_config_file(attribute, config, compulsory=False):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:

    """
    if attribute in config and (config[attribute] != ""):
        return config[attribute]
    elif compulsory:
        print("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        print("{} not specified in config file.".format(attribute))
        return None


def check_time_format(time):
    try:
        time = datetime.strptime(time, DATE_TIME_FORMAT)

        if time.strftime('%S') != '00':
            print("Seconds should be always 00")
            exit(1)
        if time.strftime('%M') != '00':
            print("Minutes should be always 00")
            exit(1)

        return True
    except Exception:
        print("Time {} is not in proper format".format(time))
        exit(1)


def prepare_inflow(inflow_file_path, start, end, discharge_id, wl_id):
    obs_wl = None

    try:

        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                                 port=CURW_SIM_PORT,
                                 db=CURW_SIM_DATABASE)

        curw_obs_pool = get_Pool(host=CURW_OBS_HOST, user=CURW_OBS_USERNAME, password=CURW_OBS_PASSWORD,
                                 port=CURW_OBS_PORT,
                                 db=CURW_OBS_DATABASE)

        connection = curw_obs_pool.connection()

        # Extract discharge series
        with connection.cursor() as cursor1:
            obs_end = datetime.strptime(start, DATE_TIME_FORMAT) + timedelta(hours=10)
            cursor1.callproc('getWL', (wl_id, start, obs_end))
            result = cursor1.fetchone()
            obs_wl = result.get('value')

        if obs_wl is None:
            obs_wl = 0.5

        TS = DisTS(pool=curw_sim_pool)
        discharge_ts = TS.get_timeseries(id_=discharge_id, start_date=start, end_date=end)

        inflow = []

        inflow.append('0               0')
        inflow.append('C               0            8655')
        inflow.append('H               0               0')

        timeseries = discharge_ts
        for i in range(1, len(timeseries)):
            time_col = (str('%.1f' % (((timeseries[i][0] - timeseries[0][0]).total_seconds()) / 3600))).rjust(16)
            value_col = (str('%.1f' % (timeseries[i][1]))).rjust(16)
            inflow.append('H' + time_col + value_col)

        inflow.append('R            2265{}'.format((str(obs_wl)).rjust(16)))
        inflow.append('R            3559             6.6')

        write_to_file(inflow_file_path, data=inflow)

    except Exception as e:
        print(traceback.print_exc())
    finally:
        connection.close()
        destroy_Pool(curw_obs_pool)
        destroy_Pool(curw_sim_pool)
        print("Inflow generated")


def create_dir_if_not_exists(path):
    """
    create directory(if needed recursively) or paths
    :param path: string : directory path
    :return: string
    """
    if not os.path.exists(path):
        os.makedirs(path)

    return path


def get_ts_start_end_for_data_type(run_date, run_time, forward=3, backward=2):
    result = {}
    """
    method for geting timeseries start and end using input params.
    :param run_date:run_date: string yyyy-mm-ddd
    :param run_time:run_time: string hh:mm:ss
    :param forward:int
    :param backward:int
    :return: tuple (string, string)
    """
    run_datetime = datetime.strptime('%s %s' % (run_date, '00:00:00'), '%Y-%m-%d %H:%M:%S')
    ts_start_datetime = run_datetime - timedelta(days=backward)
    ts_end_datetime = run_datetime + timedelta(days=forward)
    run_datetime = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    result['obs_start'] = ts_start_datetime
    result['run_time'] = run_datetime
    result['forecast_time'] = ts_end_datetime
    print(result)
    return result


def create_inflow(dir_path, run_date, run_time, forward=3, backward=2, model='flo2d_250'):
    print('create_inflow|[run_date, run_time, forward, backward] : ', [run_date, run_time, forward, backward])
    try:
        time_limits = get_ts_start_end_for_data_type(run_date, run_time, forward, backward)
        # Load config details and db connection params
        print('create_inflow|cwd : ', os.getcwd())
        config_path = os.path.join(os.getcwd(), 'inflowdat', 'config.json')
        config = json.loads(open(config_path).read())

        output_dir = dir_path
        file_name = read_attribute_from_config_file('output_file_name', config)

        discharge_id = read_attribute_from_config_file('discharge_id', config, True)
        wl_id = read_attribute_from_config_file('wl_id', config, True)

        start_time = time_limits['obs_start']
        end_time = time_limits['forecast_time']

        inflow_file_path = os.path.join(output_dir, file_name)

        print("{} start preparing inflow".format(datetime.now()))
        prepare_inflow(inflow_file_path, start=start_time.strftime('%Y-%m-%d %H:%M:%S'), end=end_time.strftime(
            '%Y-%m-%d %H:%M:%S'), discharge_id=discharge_id, wl_id=wl_id)
        print("{} completed preparing inflow".format(datetime.now()))

    except Exception:
        traceback.print_exc()

