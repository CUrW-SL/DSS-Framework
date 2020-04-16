from decimal import Decimal
from functools import reduce
from sklearn.metrics import mean_squared_error
import pandas as pd
import pymysql
import os
import matplotlib.pyplot as plt
from accuracy_unit.db_plugin import get_wrf_basin_stations, \
    get_wrf_station_hash_ids, get_station_timeseries, get_latest_fgt, \
    get_obs_basin_stations, get_obs_station_hash_ids, get_obs_station_timeseries

RESOURCE_PATH = '/home/hasitha/PycharmProjects/DSS-Framework/resources/shape_files'

# connection params
HOST = "35.227.163.211"
USER = "admin"
PASSWORD = "floody"
SIM_DB = "curw_sim"
FCST_DB = "curw_fcst"
OBS_DB = "curw_obs"
PORT = 3306


def calculate_wrf_model_mean(sim_tag, wrf_model, start_time, end_time):
    print('calculate_wrf_model_mean|[sim_tag, wrf_model, start_time, end_time]: ',
          [sim_tag, wrf_model, start_time, end_time])
    fcst_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=FCST_DB,
                                      cursorclass=pymysql.cursors.DictCursor)
    obs_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=OBS_DB,
                                     cursorclass=pymysql.cursors.DictCursor)
    shape_file = os.path.join(RESOURCE_PATH, 'Kalani_basin_hec_wgs/Kalani_basin_hec_wgs.shp')
    obs_cum_mean_df = get_obs_cum_mean_df(obs_connection, shape_file, start_time, end_time)
    if obs_cum_mean_df is not None:
        fcst_cum_mean_df = get_fcst_cum_mean_df(fcst_connection, shape_file, sim_tag, wrf_model, start_time, end_time)
        if fcst_cum_mean_df is not None:
            obs_cum_mean_df = pd.DataFrame({'time': obs_cum_mean_df.index, 'observed': obs_cum_mean_df.values})
            fcst_cum_mean_df = pd.DataFrame({'time': fcst_cum_mean_df.index, 'forecast': fcst_cum_mean_df.values})
            [formatted_obs_cum_mean_df, formatted_fcst_cum_mean_df] = get_common_start_end(obs_cum_mean_df, fcst_cum_mean_df)
            compare_cum_mean_df = pd.merge(formatted_obs_cum_mean_df, formatted_fcst_cum_mean_df, left_on='time', right_on='time')
            compare_cum_mean_df.observed = pd.to_numeric(compare_cum_mean_df.observed)
            compare_cum_mean_df.forecast = pd.to_numeric(compare_cum_mean_df.forecast)
            # print('calculate_wrf_model_mean|compare_cum_mean_df : ', compare_cum_mean_df)
            rmse = ((compare_cum_mean_df.observed - compare_cum_mean_df.forecast) ** 2).mean() ** .5
            print('calculate_wrf_model_mean|{sim_tag, wrf_model, rmse} : ', {sim_tag, wrf_model, rmse})


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
        print(basin_points)
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
            obs_cum_mean_df.to_csv('/home/hasitha/PycharmProjects/DSS-Framework/output/obs_cum_mean_df.csv')
            return obs_cum_mean_df


def get_fcst_cum_mean_df(fcst_connection, shape_file, sim_tag, wrf_model, start_time, end_time):
    basin_points = get_wrf_basin_stations(fcst_connection, shape_file)
    total_df = None
    station_count = 0
    if len(basin_points) > 0:
        print(basin_points)
        hash_ids = get_wrf_station_hash_ids(fcst_connection, sim_tag, wrf_model, basin_points)
        latest_fgt = get_latest_fgt(fcst_connection, hash_ids[0], start_time)
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
            fcst_cum_mean_df.to_csv('/home/hasitha/PycharmProjects/DSS-Framework/output/fcst_cum_mean_df.csv')
            return fcst_cum_mean_df


if __name__ == '__main__':
    try:
        run_datetime = '2020-03-10 08:00:00'
        calculate_wrf_model_mean('gfs_d0_18', 19, '2020-01-03 00:00:00', '2020-01-04 00:00:00')
        calculate_wrf_model_mean('gfs_d0_18', 20, '2020-01-03 00:00:00', '2020-01-04 00:00:00')
        calculate_wrf_model_mean('gfs_d0_18', 21, '2020-01-03 00:00:00', '2020-01-04 00:00:00')
        calculate_wrf_model_mean('gfs_d0_18', 22, '2020-01-03 00:00:00', '2020-01-04 00:00:00')
    except Exception as e:
        print(str(e))
