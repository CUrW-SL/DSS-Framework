from decimal import Decimal
from shapely.geometry import Polygon, Point, shape
import pandas as pd
from datetime import datetime, timedelta
import fiona


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
    cur = fcst_connection.cursor()
    cur.callproc('getLatestFGTs', (hash_id, start_time))
    rows = cur.fetchall()
    print('get_latest_fgts|rows: ', rows)
    return rows[0]['fgt'].strftime('%Y-%m-%d %H:%M:%S')


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
                'station in ({}) and end_date>\'{}\';'.format(basin_points_str,start_time)
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
                print('data_error : {}'.format(data_error))
                print('filling missing data.')
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
                print('data_error : {}'.format(data_error))
                print('Data error is too large')
                return None
        else:
            print('No data.')
            return None
    except Exception as e:
        print('get_timeseries_by_id|data fetch|Exception:', e)
        return None