from decimal import Decimal
from scipy.spatial import Voronoi
from shapely.geometry import Polygon, Point, shape
import geopandas as gpd
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


def get_station_timeseries(fcst_connection, hash_id, start_time, end_time):
    sql_query = 'select time,value from curw_fcst.data where id=\'{}\' and time>=\'{}\' ' \
                'and time<=\'{}\''.format(hash_id, start_time, end_time)
    rows = get_multiple_result(fcst_connection, sql_query)
    df = pd.DataFrame(data=rows, columns=['time', 'value']).set_index(keys='time')
    return df
