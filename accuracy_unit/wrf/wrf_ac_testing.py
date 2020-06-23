import pandas as pd
import pymysql
import os
import sys
from decimal import Decimal
from shapely.geometry import Polygon, Point, shape
from datetime import datetime, timedelta
import fiona
import time
from memory_profiler import memory_usage, profile


HOST = "35.197.98.125"
USER = "admin"
PASSWORD = "floody"
SIM_DB = "curw_sim"
FCST_DB = "curw_fcst"
OBS_DB = "curw_obs"
PORT = 3306
RESOURCE_PATH = '/home/hasitha/PycharmProjects/DSS-Framework/resources/shape_files'


def create_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def get_single_result(db_connection, query):
    cur = db_connection.cursor()
    cur.execute(query)
    row = cur.fetchone()
    return row


def get_multiple_result(db_connection, query):
    cur = db_connection.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    return rows


def get_latest_fgt(fcst_connection, hash_id, start_time):
    # print('get_latest_fgt|[hash_id, start_time] : ', [hash_id, start_time])
    cur = fcst_connection.cursor()
    cur.callproc('getLatestFGTs', (hash_id, start_time))
    rows = cur.fetchall()
    if rows is not None:
        if rows:
            # print('get_latest_fgts|rows: ', rows)
            return rows[0]['fgt'].strftime('%Y-%m-%d %H:%M:%S')
        else:
            return None
    else:
        return None


def is_inside_basin(shape_file, latitude, longitude):
    multipol = fiona.open(shape_file)
    multi = next(iter(multipol))
    point = Point(longitude, latitude)
    return point.within(shape(multi['geometry']))


def get_wrf_basin_stations(fcst_connection, shape_file):
    basin_points = []
    sql_query = 'select id,latitude,longitude from curw_fcst.station where description=\'WRF point\' ' \
                'and latitude between 6.6 and 7.4 and longitude between 79.6 and 81.0'
    rows = get_multiple_result(fcst_connection, sql_query)
    for row in rows:
        latitude = Decimal(row['latitude'])
        longitude = Decimal(row['longitude'])
        if is_inside_basin(shape_file, latitude, longitude):
            basin_points.append(row['id'])
    return basin_points


def get_wrf_station_hash_ids(fcst_connection, sim_tag, wrf_model_id, basin_points):
    hash_ids = []
    basin_points_str = ','.join(str(x) for x in basin_points)
    sql_query = 'select id from curw_fcst.run where sim_tag=\'{}\' and source={} and ' \
                'station in ({})'.format(sim_tag, wrf_model_id, basin_points_str)
    rows = get_multiple_result(fcst_connection, sql_query)
    for row in rows:
        hash_ids.append(row['id'])
    return hash_ids


def get_station_timeseries(fcst_connection, hash_id, latest_fgt, start_time, end_time):
    sql_query = 'select time,value from curw_fcst.data where id=\'{}\' and time>=\'{}\' ' \
                'and time<=\'{}\' and fgt=\'{}\''.format(hash_id, start_time, end_time, latest_fgt)
    rows = get_multiple_result(fcst_connection, sql_query)
    df = pd.DataFrame(data=rows, columns=['time', 'value'])
    df['time'] = pd.to_datetime(df['time'])
    resample_df = (df.set_index('time').resample('5T').first().reset_index().reindex(columns=df.columns))
    interpolate_df = resample_df.interpolate(method='linear', limit_direction='forward')
    interpolate_df['value'].fillna(Decimal(0.0), inplace=True)
    return interpolate_df.set_index(keys='time')


def get_obs_basin_stations(obs_connection, shape_file):
    basin_points = []
    sql_query = 'select id,latitude,longitude from curw_obs.station where station_type=\'CUrW_WeatherStation\' ' \
                'and longitude between 79.6 and 81.0;'
    rows = get_multiple_result(obs_connection, sql_query)
    for row in rows:
        latitude = Decimal(row['latitude'])
        longitude = Decimal(row['longitude'])
        if is_inside_basin(shape_file, latitude, longitude):
            basin_points.append(row['id'])
    return basin_points


def get_obs_station_hash_ids(obs_connection, basin_points, start_time):
    hash_ids = []
    basin_points_str = ','.join(str(x) for x in basin_points)
    sql_query = 'select id from curw_obs.run where variable=10 and unit=9 and ' \
                'station in ({}) and end_date>\'{}\';'.format(basin_points_str, start_time)
    rows = get_multiple_result(obs_connection, sql_query)
    for row in rows:
        hash_ids.append(row['id'])
    return hash_ids


def get_obs_station_timeseries(obs_connection, hash_id, timeseries_start, timeseries_end, max_error):
    data_sql = 'select time,value from curw_obs.data where id=\'{}\' and time >= \'{}\' ' \
               'and time <= \'{}\''.format(hash_id, timeseries_start, timeseries_end)
    try:
        results = get_multiple_result(obs_connection, data_sql)
        if len(results) > 0:
            time_step_count = int((datetime.strptime(timeseries_end, '%Y-%m-%d %H:%M:%S')
                                   - datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S')).total_seconds() / (
                                          60 * 5))
            data_error = ((time_step_count - len(results)) / time_step_count)
            if data_error < 0:
                df = pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
                return df
            elif data_error < max_error:
                # print('data_error : {}'.format(data_error))
                # print('filling missing data.')
                formatted_ts = []
                i = 0
                for step in range(time_step_count + 1):
                    tms_step = datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S') + timedelta(
                        minutes=step * 5)
                    if step < len(results):
                        if tms_step == results[i]['time']:
                            formatted_ts.append(results[i])
                        else:
                            formatted_ts.append({'time': tms_step, 'value': Decimal(0)})
                    else:
                        formatted_ts.append({'time': tms_step, 'value': Decimal(0)})
                    i += 1
                df = pd.DataFrame(data=formatted_ts, columns=['time', 'value']).set_index(keys='time')
                # print('get_station_timeseries|df: ', df)
                return df
            else:
                # print('data_error : {}'.format(data_error))
                # print('Data error is too large')
                return None
        else:
            # print('No data.')
            return None
    except Exception as e:
        print('get_timeseries_by_id|data fetch|Exception:', e)
        return None


def get_obs_water_levels(db_connection, start_date, end_date, model='flo2d_150', grid_id='waterlevel_glencourse',
                         method='SF'):
    id_query = 'select id from curw_sim.wl_run where model=\'{}\' and method=\'{}\' and ' \
               'grid_id=\'{}\';'.format(model, method, grid_id)
    print('get_obs_water_levels|id_query : ', id_query)
    id_result = get_single_result(db_connection, id_query)
    print('get_obs_water_levels|id_result : ', id_result)
    hash_id = id_result['id']
    print('get_obs_water_levels|hash_id : ', hash_id)
    if hash_id is not None:
        data_query = 'select time,value from curw_sim.wl_data where id=\'{}\' and time>\'{}\' ' \
                     'and time<\'{}\';'.format(hash_id, start_date, end_date)
        print('get_obs_water_levels|data_query : ', data_query)
        data_result = get_multiple_result(db_connection, data_query)
        print('get_obs_water_levels|data_result : ', data_result)
        return data_result
    return None


def get_discharge_fcst_df(db_connection, start_date, end_date, sim_tag='event_run', source_id=17):
    id_query = 'select id from curw_fcst.run where variable=3 and unit=3 and source={} and sim_tag=\'{}\';'.format(
        source_id, sim_tag)
    print('get_discharge_fcst_df|id_query : ', id_query)
    id_result = get_single_result(db_connection, id_query)
    print('get_discharge_fcst_df|id_result : ', id_result)
    hash_id = id_result['id']
    print('get_discharge_fcst_df|hash_id : ', hash_id)
    if hash_id is not None:
        data_query = 'select time,value from curw_fcst.data where id=\'{}\' and time>\'{}\' ' \
                     'and time<\'{}\';'.format(hash_id, start_date, end_date)
        print('get_discharge_fcst_df|data_query : ', data_query)
        data_result = get_multiple_result(db_connection, data_query)
        print('get_discharge_fcst_df|data_result : ', data_result)
        df = pd.DataFrame(data=data_result, columns=['time', 'value']).set_index(keys='time')
        df.rename(columns={'value': 'forecast'}, inplace=True)
        return df
    return None


def get_common_start_end(obs_cum_mean_df, fcst_cum_mean_df):
    if len(obs_cum_mean_df.index) - len(fcst_cum_mean_df.index) > 0:
        smallest_df = fcst_cum_mean_df
    else:
        smallest_df = obs_cum_mean_df
    start = smallest_df.iloc[0]['time']
    end = smallest_df.iloc[-1]['time']
    obs_cum_mean_df1 = obs_cum_mean_df[obs_cum_mean_df['time'] >= start]
    obs_cum_mean_df2 = obs_cum_mean_df1[obs_cum_mean_df1['time'] <= end]
    fcst_cum_mean_df1 = fcst_cum_mean_df[fcst_cum_mean_df['time'] >= start]
    fcst_cum_mean_df2 = fcst_cum_mean_df1[fcst_cum_mean_df1['time'] <= end]
    return [obs_cum_mean_df2, fcst_cum_mean_df2]


def get_obs_cum_mean_df(obs_connection, shape_file, start_time, end_time, max_error=0.7):
    basin_points = get_obs_basin_stations(obs_connection, shape_file)
    total_df = None
    station_count = 0
    if len(basin_points) > 0:
        hash_ids = get_obs_station_hash_ids(obs_connection, basin_points, start_time)
        for hash_id in hash_ids:
            df = get_obs_station_timeseries(obs_connection, hash_id, start_time, end_time, max_error)
            if df is not None:
                if station_count == 0:
                    total_df = df
                else:
                    total_df = total_df.add(df, fill_value=0)
                station_count += 1
        if total_df is not None:
            obs_mean_df = total_df['value'] / station_count
            obs_cum_mean_df = obs_mean_df.cumsum()
            return obs_cum_mean_df


def get_fcst_cum_mean_df(fcst_connection, shape_file, sim_tag, wrf_model, start_time, end_time):
    basin_points = get_wrf_basin_stations(fcst_connection, shape_file)
    total_df = None
    station_count = 0
    if len(basin_points) > 0:
        hash_ids = get_wrf_station_hash_ids(fcst_connection, sim_tag, wrf_model, basin_points)
        latest_fgt = get_latest_fgt(fcst_connection, hash_ids[0], start_time)
        if latest_fgt is not None:
            for hash_id in hash_ids:
                df = get_station_timeseries(fcst_connection, hash_id, latest_fgt, start_time, end_time)
                if df is not None:
                    if station_count == 0:
                        total_df = df
                    else:
                        total_df = total_df.add(df, fill_value=0)
                    station_count += 1
            if total_df is not None:
                fcst_mean_df = total_df['value'] / station_count
                fcst_cum_mean_df = fcst_mean_df.cumsum()
                return fcst_cum_mean_df
        else:
            return None


def calculate_wrf_model_mean(sim_tag, wrf_model, start_time, end_time):
    # print('calculate_wrf_model_mean|[sim_tag, wrf_model, start_time, end_time]: ',
    #       [sim_tag, wrf_model, start_time, end_time])
    rmse_params = {}
    fcst_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=FCST_DB,
                                      cursorclass=pymysql.cursors.DictCursor)
    obs_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=OBS_DB,
                                     cursorclass=pymysql.cursors.DictCursor)
    shape_file = os.path.join(RESOURCE_PATH, 'Kalani_basin_hec_wgs/Kalani_basin_hec_wgs.shp')
    obs_cum_mean_df = get_obs_cum_mean_df(obs_connection, shape_file, start_time, end_time)
    if obs_cum_mean_df is not None and obs_cum_mean_df.empty is False:
        fcst_cum_mean_df = get_fcst_cum_mean_df(fcst_connection, shape_file, sim_tag, wrf_model, start_time, end_time)
        if fcst_cum_mean_df is not None and fcst_cum_mean_df.empty is False:
            obs_cum_mean_df = pd.DataFrame({'time': obs_cum_mean_df.index, 'observed': obs_cum_mean_df.values})
            fcst_cum_mean_df = pd.DataFrame({'time': fcst_cum_mean_df.index, 'forecast': fcst_cum_mean_df.values})
            [formatted_obs_cum_mean_df, formatted_fcst_cum_mean_df] = get_common_start_end(obs_cum_mean_df,
                                                                                           fcst_cum_mean_df)
            compare_cum_mean_df = pd.merge(formatted_obs_cum_mean_df, formatted_fcst_cum_mean_df, left_on='time',
                                           right_on='time')
            test_result_dir = os.path.join('/home/hasitha/PycharmProjects/DSS-Framework/output',
                                           '{}'.format(datetime.now().strftime('%Y-%m-%d')))
            create_dir(test_result_dir)
            fcst_cum_mean_file = os.path.join(test_result_dir, 'fcst_{}_{}_cum_mean.csv'.format(sim_tag, wrf_model))
            formatted_fcst_cum_mean_df.to_csv(fcst_cum_mean_file, encoding='utf-8', index=False)
            obs_cum_mean_file = os.path.join(test_result_dir, 'obs_cum_mean.csv')
            formatted_obs_cum_mean_df.to_csv(obs_cum_mean_file, encoding='utf-8', index=False)

            compare_cum_mean_df.observed = pd.to_numeric(compare_cum_mean_df.observed)
            compare_cum_mean_df.forecast = pd.to_numeric(compare_cum_mean_df.forecast)
            rmse = ((compare_cum_mean_df.observed - compare_cum_mean_df.forecast) ** 2).mean() ** .5
            rmse_params = {'sim_tag': sim_tag, 'wrf_model': wrf_model, 'rmse': rmse}
            # print('calculate_wrf_model_mean|rmse_params : ', rmse_params)
    fcst_connection.close()
    obs_connection.close()
    return rmse_params


def get_obs_and_fcst_times(test_date, gfs_hour):
    # print('get_obs_and_fcst_times|[test_date, gfs_hour] : ', [test_date, gfs_hour])
    gfs_hour = int(gfs_hour)
    test_date = datetime.strptime(test_date, '%Y-%m-%d')
    test_date = test_date + timedelta(hours=gfs_hour + 5, minutes=30)
    ts_start = test_date - timedelta(hours=23, minutes=45)
    ts_end = test_date + timedelta(hours=48)
    # print('get_obs_and_fcst_times|ts_start : ', ts_start)
    # print('get_obs_and_fcst_times|test_date : ', test_date)
    # print('get_obs_and_fcst_times|ts_end : ', ts_end)
    ts_start_str = ts_start.strftime('%Y-%m-%d %H:%M:%S')
    test_date_str = test_date.strftime('%Y-%m-%d %H:%M:%S')
    ts_end_str = ts_end.strftime('%Y-%m-%d %H:%M:%S')
    return [ts_start_str, test_date_str, ts_end_str]


@profile
def wrf_model_selection(sim_tag, test_date, gfs_hour, model_ids):
    t0 = time.time()
    [start_date, test_date, end_date] = get_obs_and_fcst_times(test_date, gfs_hour)
    selected = {}
    for model_id in model_ids:
        model_result = calculate_wrf_model_mean(sim_tag, model_id, start_date, test_date)
        print('wrf_model_selection|model_result : ', model_result)
        if model_result:
            if selected:
                if selected['rmse'] > model_result['rmse'] :
                    selected = model_result
            else:
                selected = model_result
    print('wrf_model_selection|selected: ', selected)
    t1 = time.time()
    print("Total time running %s seconds" %(str(t1 - t0)))
    return selected


def measure_accuracy(sim_tag, test_date, gfs_hour, model_id):
    [start_date, test_date, end_date] = get_obs_and_fcst_times(test_date, gfs_hour)
    model_result = calculate_wrf_model_mean(sim_tag, model_id, start_date, test_date)
    print('measure_accuracy|model_result : ', model_result)


if __name__ == "__main__":
    sim_tag = 'dwrf_gfs_d1_18'
    test_date = '2020-05-10'
    gfs_hour = '18'
    # mem_usage = memory_usage(wrf_model_selection('dwrf_gfs_d1_18', '2020-05-10', '18', [19, 20, 21, 22]))
    selected = wrf_model_selection(sim_tag, test_date, gfs_hour, [19, 20, 21, 22])
    print('selected model : ', selected)
    measure_accuracy(sim_tag, test_date, gfs_hour, selected['wrf_model'])


