import pymysql
from datetime import datetime, timedelta
import traceback
from flo2d.db_plugin import get_cell_mapping, select_distinct_observed_stations, \
    select_obs_station_precipitation_for_timestamp, select_fcst_station_precipitation_for_timestamp, \
    select_distinct_forecast_stations
import os
from flo2d.utils import search_value_in_dictionary_list
import pandas as pd

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
INVALID_VALUE = -9999

# connection params
HOST = "35.227.163.211"
USER = "admin"
PASSWORD = "floody"
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
    print("************Connected to database**************")

    print('prepare_raincell|[start_time, end_time, target_model] : ', [start_time, end_time, target_model])
    # end_time = datetime.strptime(end_time, DATE_TIME_FORMAT)
    # start_time = datetime.strptime(start_time, DATE_TIME_FORMAT)

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
    print('prepare_raincell| [min_start_time, max_end_time] :', [min_start_time, max_end_time])

    if end_time > max_end_time:
        end_time = max_end_time

    if start_time < min_start_time:
        start_time = min_start_time

    if target_model == "flo2d_250":
        timestep = 5
    elif target_model == "flo2d_150":
        timestep = 15

    print('prepare_raincell|[start_time, end_time, timestep] :', [start_time, end_time, timestep])
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


def generate_raincell(raincell_file_path, time_limits, model, data_type, wrf_model, sim_tag=None):
    print('[raincell_file_path, time_limits, model, data_type, wrf_model, sim_tag] : ', [raincell_file_path, time_limits, model, data_type,
                                                                              wrf_model, sim_tag])
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
    print('generate_raincell|raincell_file_path : ', raincell_file_path)
    grid_maps = get_cell_mapping(sim_connection, model)
    obs_station_id_list = select_distinct_observed_stations(sim_connection, model)
    obs_station_ids = ','.join(str(i) for i in obs_station_id_list)
    print('obs_station_ids : ', obs_station_ids)
    fcst_station_id_list = select_distinct_forecast_stations(sim_connection, model)
    fcst_station_ids = ','.join(str(i) for i in fcst_station_id_list)
    print('fcst_station_ids : ', fcst_station_ids)
    if data_type == 1:  # Observed only
        try:
            timestamp = start_time
            while timestamp < run_time:
                timestamp = timestamp + timedelta(minutes=timestep)
                obs_station_precipitations = select_obs_station_precipitation_for_timestamp(obs_connection,
                                                                                            obs_station_ids,
                                                                                            timestamp.strftime(
                                                                                                DATE_TIME_FORMAT))
                raincell_entries = get_obs_raincell_entries_for_timestamp(grid_maps, obs_station_precipitations)
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
            print("{} observed raincell generation process completed".format(datetime.now()))
    elif data_type == 2:  # Forecast only
        try:
            timestamp = start_time
            while timestamp < end_time:
                timestamp = timestamp + timedelta(minutes=timestep)
                minutes = timestamp.strftime('%M')
                if minutes == '00' or minutes == '15' or minutes == '30' or minutes == '45':
                    print('15 min intervals')
                    fcst_station_precipitations = select_fcst_station_precipitation_for_timestamp(fcst_connection,
                                                                                                  fcst_station_ids,
                                                                                                  timestamp.strftime(
                                                                                                      DATE_TIME_FORMAT),
                                                                                                  wrf_model,
                                                                                                  sim_tag)
                    raincell_entries = get_fcst_raincell_entries_for_timestamp(grid_maps, fcst_station_precipitations)
                    for i in range(3):
                        if len(raincell_entries) > 0:
                            append_to_file(raincell_file_path, raincell_entries)
                print(timestamp)
        except Exception as ex:
            traceback.print_exc()
        finally:
            sim_connection.close()
            fcst_connection.close()
            obs_connection.close()
            print("{} forecast raincell generation process completed".format(datetime.now()))
    elif data_type == 3:  # Observed + Forecast
        try:
            timestamp = start_time
            while timestamp < run_time:
                timestamp = timestamp + timedelta(minutes=timestep)
                obs_station_precipitations = select_obs_station_precipitation_for_timestamp(obs_connection,
                                                                                            obs_station_ids,
                                                                                            timestamp.strftime(
                                                                                                DATE_TIME_FORMAT))
                raincell_entries = get_obs_raincell_entries_for_timestamp(grid_maps, obs_station_precipitations)
                if len(raincell_entries) > 0:
                    append_to_file(raincell_file_path, raincell_entries)
                print(timestamp)
            while timestamp < end_time:
                timestamp = timestamp + timedelta(minutes=timestep)
                minutes = timestamp.strftime('%M')
                if minutes == '00' or minutes == '15' or minutes == '30' or minutes == '45':
                    print('15 min intervals')
                    fcst_station_precipitations = select_fcst_station_precipitation_for_timestamp(fcst_connection,
                                                                                                  fcst_station_ids,
                                                                                                  timestamp.strftime(
                                                                                                      DATE_TIME_FORMAT),
                                                                                                  wrf_model,
                                                                                                  sim_tag)
                    raincell_entries = get_fcst_raincell_entries_for_timestamp(grid_maps, fcst_station_precipitations)
                    for i in range(3):
                        if len(raincell_entries) > 0:
                            append_to_file(raincell_file_path, raincell_entries)
                print(timestamp)
        except Exception as ex:
            traceback.print_exc()
        finally:
            sim_connection.close()
            fcst_connection.close()
            obs_connection.close()
            print("{} hybrid raincell generation process completed".format(datetime.now()))
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


def get_obs_raincell_entries_for_timestamp(grid_maps, obs_station_precipitations):
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
        raincell_entries.append(raincell_entry)
    raincell_entries.append('')
    return raincell_entries


def get_fcst_raincell_entries_for_timestamp(grid_maps, fcst_station_precipitations):
    raincell_entries = []
    for grid_map in grid_maps:
        grid_id = int(grid_map['grid_id'].split('_')[3])
        fcst_id = grid_map['fcst']
        precipitation = search_value_in_dictionary_list(fcst_station_precipitations, 'station_id', fcst_id,
                                                        'step_value')
        precipitation = precipitation / 3
        if precipitation == INVALID_VALUE:
            precipitation = 0
        raincell_entry = '{} {}'.format(grid_id, '%.1f' % precipitation)
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


def create_event_raincell(dir_path, run_date, run_time, forward, backward, model, sim_tag='dwrf_gfs_d1_18', wrf_model=20, data_type=1):
    time_limits = get_ts_start_end_for_data_type(run_date, run_time, forward, backward)
    raincell_file_path = os.path.join(dir_path, 'RAINCELL.DAT')
    print('create_event_raincell|time_limits : ', time_limits)
    print('create_event_raincell|raincell_file_path : ', raincell_file_path)
    start_time = datetime.now()
    if not os.path.isfile(raincell_file_path):
        generate_raincell(raincell_file_path, time_limits, model, data_type, wrf_model, sim_tag)
    else:
        print('Raincell file already in path : ', raincell_file_path)
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60
    print('create_raincell|duration : ', duration)


def get_cell_mapping(sim_connection, flo2d_model):
    if 'flo2d_150' == flo2d_model:
        query = 'select grid_id,obs1,obs2,obs3,fcst from curw_sim.grid_map_flo2d_raincell where grid_id like \"flo2d_150_%\";'
    elif 'flo2d_250' == flo2d_model:
        query = 'select grid_id,obs1,obs2,obs3,fcst from curw_sim.grid_map_flo2d_raincell where grid_id like \"flo2d_250_%\";'
    print('get_cell_mapping|query : ', query)
    rows = get_multiple_result(sim_connection, query)
    print('get_cell_mapping|rows : ', rows)
    cell_map = pd.DataFrame(data=rows, columns=['grid_id', 'obs1', 'obs2', 'obs3', 'fcst'])
    print('get_cell_mapping|cell_map : ', cell_map)
    return rows


def select_distinct_observed_stations(obs_connection, flo2d_model):
    if 'flo2d_150' == flo2d_model:
        query = 'select distinct(obs1) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_150_%" union ' \
                'select distinct(obs2) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_150_%" union ' \
                'select distinct(obs3) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_150_%";'
    elif 'flo2d_250' == flo2d_model:
        query = 'select distinct(obs1) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_250_%" union ' \
                'select distinct(obs2) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_250_%" union ' \
                'select distinct(obs3) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_250_%";'
    print('select_distinct_observed_stations|query : ', query)
    rows = get_multiple_result(obs_connection, query)
    # print('select_distinct_observed_stations|rows : ', rows)
    id_list = []
    for row in rows:
        id_list.append(row['obs1'])
    print('select_distinct_observed_stations|id_list : ', id_list)
    return id_list


def select_obs_station_precipitation_for_timestamp(obs_connection, station_ids, time_step):
    query = 'select station_tbl.station_id,time_tbl.step_value from ' \
            '(select id as hash_id, station as station_id from curw_obs.run where unit=9 and variable=10 and station in ({})) station_tbl,' \
            '(select id as hash_id, value as step_value from curw_obs.data where time=\'{}\') time_tbl ' \
            'where station_tbl.hash_id = time_tbl.hash_id;'.format(station_ids, time_step)
    print('select_obs_station_precipitation_for_timestamp|query : ', query)
    rows = get_multiple_result(obs_connection, query)
    return rows


def select_distinct_forecast_stations(fcst_connection, flo2d_model):
    if 'flo2d_150' == flo2d_model:
        query = 'select distinct(fcst) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_150_%" ;'
    elif 'flo2d_250' == flo2d_model:
        query = 'select distinct(fcst) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_250_%" ;'
    print('select_distinct_forecast_stations|query : ', query)
    rows = get_multiple_result(fcst_connection, query)
    # print('select_distinct_observed_stations|rows : ', rows)
    id_list = []
    for row in rows:
        id_list.append(row['fcst'])
    print('select_distinct_forecast_stations|id_list : ', id_list)
    return id_list


def select_fcst_station_precipitation_for_timestamp(fcst_connection, station_ids, time_step, wrf_model,
                                                    sim_tag='dwrf_gfs_d1_18'):
    query = 'select station_tbl.station_id,time_tbl.step_value from ' \
            '(select id as hash_id, station as station_id from curw_fcst.run where unit=1 and variable=1 ' \
            'and sim_tag=\'{}\' and source={} and station in ({})) station_tbl,' \
            '(select id as hash_id, value as step_value from curw_fcst.data where time=\'{}\') time_tbl ' \
            'where station_tbl.hash_id = time_tbl.hash_id;'.format(sim_tag, wrf_model, station_ids, time_step)
    print('select_fcst_station_precipitation_for_timestamp|query : ', query)
    rows = get_multiple_result(fcst_connection, query)
    return rows


def get_single_result(sim_connection, query):
    cur = sim_connection.cursor()
    cur.execute(query)
    row = cur.fetchone()
    return row


def get_multiple_result(sim_connection, query):
    cur = sim_connection.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    return rows


if __name__ == '__main__':
    try:
        create_event_raincell('/home/hasitha/PycharmProjects/DSS-Framework/output',
                        '2020-03-10', '08:00:00', 3, 2, 'flo2d_250', 'dwrf_gfs_d1_18', 19)
    except Exception as e:
        print(str(e))
