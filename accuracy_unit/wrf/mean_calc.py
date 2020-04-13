import pymysql
import os
from accuracy_unit.db_plugin import get_wrf_basin_stations, \
    get_wrf_station_hash_ids, get_station_timeseries

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
    sim_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=SIM_DB,
                                     cursorclass=pymysql.cursors.DictCursor)
    fcst_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=FCST_DB,
                                      cursorclass=pymysql.cursors.DictCursor)
    obs_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=OBS_DB,
                                     cursorclass=pymysql.cursors.DictCursor)
    # shape_file = os.path.join(RESOURCE_PATH, 'klb-wgs84/klb-wgs84.shp')
    shape_file = os.path.join(RESOURCE_PATH, 'Kalani_basin_hec_wgs/Kalani_basin_hec_wgs.shp')
    basin_points = get_wrf_basin_stations(fcst_connection, shape_file)
    total_df = None
    station_count = 0
    if len(basin_points) > 0:
        print(basin_points)
        hash_ids = get_wrf_station_hash_ids(fcst_connection, sim_tag, wrf_model, basin_points)
        for hash_id in hash_ids:
            df = get_station_timeseries(fcst_connection, hash_id, start_time, end_time)
            if df is not None:
                if station_count == 0:
                    total_df = df
                else:
                    total_df = total_df.add(df, fill_value=0)
                station_count += 1
        if total_df is not None:
            print('calculate_wrf_model_mean|total_df : ', total_df)
            total_df.to_csv('/home/hasitha/PycharmProjects/DSS-Framework/output/rain.csv')
            mean_df = total_df['value'] / station_count
            cum_mean_df = mean_df.cumsum()
            print('calculate_wrf_model_mean|cum_mean_df : ', cum_mean_df)


if __name__ == '__main__':
    try:
        run_datetime = '2020-03-10 08:00:00'
        calculate_wrf_model_mean('dwrf_gfs_d1_18', 19, '2020-03-08 00:00:00', '2020-03-09 00:00:00')
    except Exception as e:
        print(str(e))
