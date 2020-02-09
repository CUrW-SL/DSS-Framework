import pymysql
from datetime import datetime, timedelta
import traceback
from db_plugin import get_cell_mapping, select_distinct_observed_stations, \
    select_obs_station_precipitation_for_timestamp
import os
from utils import search_value_in_dictionary_list

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
INVALID_VALUE = -9999

# connection params
# HOST = "10.138.0.13"
HOST = "35.227.163.211"
USER = "routine_user"
PASSWORD = "aquaroutine"
SIM_DB = "curw_sim"
FCST_DB = "curw_fcst"
OBS_DB = "curw_obs"
PORT = 3306


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def append_to_file(file_name, data):
    with open(file_name, 'a+') as f:
        f.write('\n'.join(data))


def insert(originalfile, string):
    with open(originalfile, 'r') as f:
        with open('newfile.txt', 'w') as f2:
            f2.write(string)
            f2.write(f.read())
    os.rename('newfile.txt', originalfile)


def prepare_raincell(raincell_file_path, start_time, end_time,
                     target_model="flo2d_250", interpolation_method="MME"):
    """
    Create raincell for flo2d
    :param raincell_file_path:
    :param start_time: Raincell start time (e.g: "2019-06-05 00:00:00")
    :param end_time: Raincell start time (e.g: "2019-06-05 23:30:00")
    :param target_model: FLO2D model (e.g. flo2d_250, flo2d_150)
    :param interpolation_method: value interpolation method (e.g. "MME")
    :return:
    """
    connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=SIM_DB,
                                 cursorclass=pymysql.cursors.DictCursor)
    print("Connected to database")

    end_time = datetime.strptime(end_time, DATE_TIME_FORMAT)
    start_time = datetime.strptime(start_time, DATE_TIME_FORMAT)

    if end_time < start_time:
        print("start_time should be less than end_time")
        exit(1)
    # find max end time
    try:
        with connection.cursor() as cursor0:
            cursor0.callproc('get_ts_end', (target_model, interpolation_method))
            max_end_time = cursor0.fetchone()['time']

    except Exception as e:
        traceback.print_exc()
        max_end_time = datetime.strptime((datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 23:30:00'),
                                         DATE_TIME_FORMAT)

    min_start_time = datetime.strptime("2019-06-28 00:00:00", DATE_TIME_FORMAT)

    if end_time > max_end_time:
        end_time = max_end_time

    if start_time < min_start_time:
        start_time = min_start_time

    if target_model == "flo2d_250":
        timestep = 5
    elif target_model == "flo2d_150":
        timestep = 15

    length = int(((end_time - start_time).total_seconds() / 60) / timestep)

    write_to_file(raincell_file_path,
                  ['{} {} {} {}\n'.format(timestep, length, start_time.strftime(DATE_TIME_FORMAT),
                                          end_time.strftime(DATE_TIME_FORMAT))])
    try:
        timestamp = start_time
        while timestamp < end_time:
            raincell = []
            timestamp = timestamp + timedelta(minutes=timestep)
            # Extract raincell from db
            with connection.cursor() as cursor1:
                cursor1.callproc('prepare_flo2d_raincell', (target_model, interpolation_method, timestamp))
                for result in cursor1:
                    raincell.append('{} {}'.format(result.get('cell_id'), '%.1f' % result.get('value')))
                raincell.append('')
            append_to_file(raincell_file_path, raincell)
            print(timestamp)
    except Exception as ex:
        traceback.print_exc()
    finally:
        connection.close()
        print("{} raincell generation process completed".format(datetime.now()))


def get_ts_start_end(run_date, run_time, forward=3, backward=2):
    result = []
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
    result.append(ts_start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    result.append(ts_end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print(result)
    return result


def create_sim_hybrid_raincell(dir_path, run_date, run_time, forward, backward,
                               res_mins=60, flo2d_model='flo2d_250', calc_method='MME'):
    [timeseries_start, timeseries_end] = get_ts_start_end(run_date, run_time)
    raincell_file_path = os.path.join(dir_path, 'RAINCELL.DAT')
    if not os.path.isfile(raincell_file_path):
        print("{} start preparing raincell".format(datetime.now()))
        prepare_raincell(raincell_file_path, target_model=flo2d_model, interpolation_method=calc_method,
                         start_time=timeseries_start, end_time=timeseries_end)
        print("{} completed preparing raincell".format(datetime.now()))
    else:
        print('Raincell file already in path : ', raincell_file_path)


def generate_raincell(raincell_file_path, time_limits, model, data_type, any_wrf=None, sim_tag=None):
    sim_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=SIM_DB,
                                     cursorclass=pymysql.cursors.DictCursor)
    fcst_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=FCST_DB,
                                      cursorclass=pymysql.cursors.DictCursor)
    obs_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=OBS_DB,
                                     cursorclass=pymysql.cursors.DictCursor)
    print("Connected to database")
    if model == "flo2d_250":
        timestep = 5
    elif model == "flo2d_150":
        timestep = 15
    start_time = time_limits['obs_start']
    run_time = time_limits['run_time']
    end_time = time_limits['forecast_time']
    length = int(((end_time - start_time).total_seconds() / 60) / timestep)

    write_to_file(raincell_file_path,
                  ['{} {} {} {}\n'.format(timestep, length, start_time.strftime(DATE_TIME_FORMAT),
                                          end_time.strftime(DATE_TIME_FORMAT))])
    grid_maps = get_cell_mapping(sim_connection, model)
    station_id_list = select_distinct_observed_stations(sim_connection, model)
    station_ids = ','.join(str(i) for i in station_id_list)
    print('station_ids : ', station_ids)
    if data_type == 1:  # Observed only
        try:
            timestamp = start_time
            while timestamp < run_time:
                timestamp = timestamp + timedelta(minutes=timestep)
                obs_station_precipitations = select_obs_station_precipitation_for_timestamp(obs_connection, station_ids,
                                                                                            timestamp.strftime(
                                                                                                DATE_TIME_FORMAT))
                raincell_entries = get_raincell_entries_for_timestamp(grid_maps, obs_station_precipitations)
                if len(raincell_entries) > 0:
                    append_to_file(raincell_file_path, raincell_entries)
                print(timestamp)
            while timestamp < end_time:
                timestamp = timestamp + timedelta(minutes=timestep)
                raincell_entries = get_empty_raincell_entries(model)
                if len(raincell_entries) > 0:
                    append_to_file(raincell_file_path, raincell_entries)
                print(timestamp)
        except Exception as ex:
            traceback.print_exc()
        finally:
            sim_connection.close()
            fcst_connection.close()
            obs_connection.close()
            print("{} raincell generation process completed".format(datetime.now()))
    elif data_type == 2:  # Forecast only
        try:
            timestamp = start_time
            if sim_tag is not None:
                print('')
            else:
                print('sim_tag is required for forecast raincell generation.')
        except Exception as ex:
            traceback.print_exc()
        finally:
            sim_connection.close()
            fcst_connection.close()
            obs_connection.close()
            print("{} raincell generation process completed".format(datetime.now()))
    elif data_type == 3:  # Observed + Forecast
        try:
            timestamp = start_time
        except Exception as ex:
            traceback.print_exc()
        finally:
            sim_connection.close()
            fcst_connection.close()
            obs_connection.close()
            print("{} raincell generation process completed".format(datetime.now()))
    else:
        print('data_type mismatched...')


def get_empty_raincell_entries(model):
    raincell_entries = []
    if model == "flo2d_250":
        cell_id = 1
        while cell_id <= 9348:
            raincell_entry = '{} {}'.format(cell_id, '0.0')
            raincell_entries.append(raincell_entry)
            cell_id += 1
    elif model == "flo2d_150":
        print('xxxxxxxxxxxx')
    raincell_entries.append('')
    return raincell_entries


def get_raincell_entries_for_timestamp(grid_maps, obs_station_precipitations):
    raincell_entries = []
    for grid_map in grid_maps:
        grid_id = int(grid_map['grid_id'].split('_')[3])
        obs1_id = grid_map['obs1']
        precipitation = search_value_in_dictionary_list(obs_station_precipitations, 'station_id', obs1_id, 'step_value')
        if precipitation == INVALID_VALUE:
            obs2_id = grid_map['obs2']
            precipitation = search_value_in_dictionary_list(obs_station_precipitations, 'station_id', obs2_id,
                                                            'step_value')
            if precipitation == INVALID_VALUE:
                obs3_id = grid_map['obs3']
                precipitation = search_value_in_dictionary_list(obs_station_precipitations, 'station_id', obs3_id,
                                                                'step_value')
                if precipitation == INVALID_VALUE:
                    precipitation = 0
        raincell_entry = '{} {}'.format(grid_id, '%.1f' % precipitation)
        # print('raincell_entry : ', raincell_entry)
        raincell_entries.append(raincell_entry)
    raincell_entries.append('')
    return raincell_entries


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


def create_raincell(dir_path, run_date, run_time, forward, backward, model, data_type, any_wrf, sim_tag):
    time_limits = get_ts_start_end_for_data_type(run_date, run_time, forward, backward)
    raincell_file_path = os.path.join(dir_path, 'RAINCELL.DAT')
    start_time = datetime.now()
    if not os.path.isfile(raincell_file_path):
        generate_raincell(raincell_file_path, time_limits, model, data_type, any_wrf, sim_tag)
    else:
        print('Raincell file already in path : ', raincell_file_path)
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60
    print('create_raincell|duration : ', duration)


if __name__ == '__main__':
    try:
        create_raincell('/home/hasitha/PycharmProjects/DSS-Framework/output',
                        '2020-01-27', '08:00:00', 3, 2, 'flo2d_250', 0, 1, 'mwrf_gfs_d0_00')
    except Exception as e:
        print(str(e))
