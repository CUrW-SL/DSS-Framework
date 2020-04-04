import os
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal
from shapely.geometry import Polygon, Point
import geopandas as gpd

RESOURCE_PATH = '/home/curw/git/DSS-Framework/resources'


def update_sub_basin_mean_rain(fcst_adapter, wrf_model, wrf_version, wrf_run, gfs_hour):
    kub_shape_file = os.path.join(RESOURCE_PATH, 'kub-wgs84/kub-wgs84.shp')
    klb_shape_file = os.path.join(RESOURCE_PATH, 'klb-wgs84/klb-wgs84.shp')
    wrf_source_id = fcst_adapter.get_wrf_source_id(wrf_model, wrf_version)
    if wrf_source_id is not None:
        print('')


def get_sub_basin_stations(fcst_adapter, basin_shape_file):
    sub_basin_stations = []
    all_basin_stations_df = fcst_adapter.get_wrf_fcst_station_ids()
    if all_basin_stations_df is not None:
        shape_attribute = ['OBJECTID', 1]
        shape_df = gpd.GeoDataFrame.from_file(basin_shape_file)
        shape_polygon_idx = shape_df.index[shape_df[shape_attribute[0]] == shape_attribute[1]][0]
        shape_polygon = shape_df['geometry'][shape_polygon_idx]
        for row in all_basin_stations_df.itertuples():
            if Point(row.longitude, row.latitude).within(shape_polygon):
                sub_basin_stations.append(row.id)
        return sub_basin_stations
    else:
        print('get_sub_basin_stations|no stations')
        return sub_basin_stations


def get_basin_mean_rain(fcst_adapter, sim_tag, source_id, exec_date):
    basin_shape_file = os.path.join(RESOURCE_PATH, 'shape_files/150m_Boundary_wgs/150m_Boundary_wgs.shp')
    basin_stations = get_basin_stations(fcst_adapter, basin_shape_file)
    if len(basin_stations) > 0:
        for basin_station in basin_stations:
            print('basin_station id : ', basin_station)
            station_hash_id = fcst_adapter.get_wrf_station_hash_id(sim_tag, source_id, basin_station)
            if station_hash_id is not None:
                fgt_limits = get_fgt_limits(exec_date)
                tms_df = fcst_adapter.get_stations_time_series(station_hash_id, fgt_limits)


def get_fgt_limits(exec_date):
    fgt_start = datetime.strptime(exec_date, '%Y-%m-%d')
    fgt_end = fgt_start + timedelta(days=1)
    return [exec_date, fgt_end.strftime('%Y-%m-%d')]


def get_basin_stations(fcst_adapter, basin_shape_file):
    basin_stations = []
    all_basin_stations_df = fcst_adapter.get_wrf_fcst_station_ids()
    if all_basin_stations_df is not None:
        shape_attribute = ['OBJECTID', 1]
        shape_df = gpd.GeoDataFrame.from_file(basin_shape_file)
        shape_polygon_idx = shape_df.index[shape_df[shape_attribute[0]] == shape_attribute[1]][0]
        shape_polygon = shape_df['geometry'][shape_polygon_idx]
        for row in all_basin_stations_df.itertuples():
            if Point(row.longitude, row.latitude).within(shape_polygon):
                basin_stations.append(row.id)
        return basin_stations
    else:
        print('get_basin_stations|no stations')
        return basin_stations
