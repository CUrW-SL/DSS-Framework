import logging
import os
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal
from shapely.geometry import Point
import geopandas as gpd
import json

LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'

MISSING_VALUE = -99999
FILL_VALUE = 0


def validate_dataframe(df, allowed_error):
    row_count = len(df.index)
    missing_count = df['value'][df['value'] == MISSING_VALUE].count()
    df_error = missing_count / row_count
    print('validate_dataframe|[row_count, missing_count, df_error]:',
          [row_count, missing_count, df_error, allowed_error])
    if df_error > allowed_error:
        print('Invalid')
        return False
    else:
        print('Valid')
        return True


class CurwSimAdapter:
    __instance = None

    @staticmethod
    def get_instance(db_config):
        """ Static access method. """
        print('get_instance|db_config : ', db_config)
        if CurwSimAdapter.__instance is None:
            CurwSimAdapter(db_config['mysql_user'], db_config['mysql_password'],
                              db_config['mysql_host'], db_config['mysql_db'],
                              db_config['log_path'])
        return CurwSimAdapter.__instance

    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db, log_path):
        """ Virtually private constructor. """
        if CurwSimAdapter.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            try:
                self.connection = mysql.connector.connect(user=mysql_user,
                                                          password=mysql_password,
                                                          host=mysql_host,
                                                          database=mysql_db)
                self.cursor = self.connection.cursor(buffered=True)
                logging.basicConfig(filename=os.path.join(log_path, 'curw_sim_db_adapter.log'),
                                    level=logging.DEBUG,
                                    format=LOG_FORMAT)
                self.log = logging.getLogger()
                CurwSimAdapter.__instance = self
            except ConnectionError as ex:
                print('ConnectionError|ex: ', ex)

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    def get_single_result(self, sql_query):
        value = None
        cursor = self.cursor
        try:
            cursor.execute(sql_query)
            result = cursor.fetchone()
            if result:
                value = result
            else:
                self.log.error('no result|query:'.format(sql_query))
        except Exception as ex:
            print('get_single_result|Exception : ', str(ex))
            self.log.error('exception|query:'.format(sql_query))
        finally:
            return value

    def get_multiple_result(self, sql_query):
        cursor = self.cursor
        try:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            if result:
                return result
            else:
                self.log.error('no result|query:'.format(sql_query))
                return None
        except Exception as ex:
            print('get_single_result|Exception : ', str(ex))
            self.log.error('exception|query:'.format(sql_query))
            return None

    def get_flo2d_tms_ids(self, model, method):
        id_date_list = []
        cursor = self.cursor
        try:
            sql = 'select id,obs_end from curw_sim.run where model=\'{}\' and method=\'{}\' '.format(model, method)
            print('sql : ', sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                id_date_list.append([row[0], row[1]])
        except Exception as e:
            print('save_init_state|Exception:', e)
        finally:
            return id_date_list

    def get_flo2d_tms_ids(self, model, method):
        id_date_list = []
        cursor = self.cursor
        try:
            sql = 'select id,grid_id,obs_end from curw_sim.run where model=\'{}\' and method=\'{}\' '.format(model,
                                                                                                             method)
            print('sql : ', sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                id_date_list.append({'hash_id': row[0], 'grid_id': row[1], 'obs_end': row[2]})
        except Exception as e:
            print('save_init_state|Exception:', e)
        finally:
            return id_date_list

    def get_cell_timeseries(self, timeseries_start, timeseries_end, hash_id, res_mins):
        cursor = self.cursor
        try:
            sql = 'select time,value from curw_sim.data where time>=\'{}\' and time<\'{}\' and id=\'{}\' '.format(
                timeseries_start, timeseries_end, hash_id)
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                # return pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
                return pd.DataFrame(data=results, columns=['time', 'value'])
            else:
                return None
        except Exception as e:
            print('get_cell_timeseries|Exception:', e)
            return None

    def get_station_timeseries(self, timeseries_start, timeseries_end, station_name, source, model='hechms',
                               value_interpolation='MME', grid_interpolation='MDPA', acceppted_error=40):
        cursor = self.cursor
        try:
            grid_id = 'rainfall_{}_{}'.format(station_name, grid_interpolation)
            sql = 'select id, obs_end from curw_sim.run where model=\'{}\' and method=\'{}\'  and grid_id=\'{}\''.format(
                model, value_interpolation, grid_id)
            print('sql : ', sql)
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                hash_id = result[0]
                print('hash_id : ', hash_id)
                data_sql = 'select time,value from curw_sim.data where time>=\'{}\' and time<=\'{}\' and id=\'{}\' '.format(
                    timeseries_start, timeseries_end, hash_id)
                try:
                    print('data_sql : ', data_sql)
                    cursor.execute(data_sql)
                    results = cursor.fetchall()
                    # print('results : ', results)
                    if len(results) > 0:
                        time_step_count = int((datetime.strptime(timeseries_end, '%Y-%m-%d %H:%M:%S')
                                               - datetime.strptime(timeseries_start,
                                                                   '%Y-%m-%d %H:%M:%S')).total_seconds() / (60 * 5))
                        print('timeseries_start : {}'.format(timeseries_start))
                        print('timeseries_end : {}'.format(timeseries_end))
                        print('time_step_count : {}'.format(time_step_count))
                        print('len(results) : {}'.format(len(results)))
                        data_error = ((time_step_count - len(results)) / time_step_count) * 100
                        if data_error < 1:
                            df = pd.DataFrame(data=results, columns=['time', 'value'])
                            return df
                        elif data_error <= acceppted_error:
                            print('data_error : {}'.format(data_error))
                            print('filling missing data.')
                            formatted_ts = []
                            i = 0
                            for step in range(time_step_count):
                                tms_step = datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S') + timedelta(
                                    minutes=step * 5)
                                if step < len(results):
                                    if tms_step == results[i][0]:
                                        formatted_ts.append(results[i])
                                    else:
                                        formatted_ts.append((tms_step, Decimal(0)))
                                else:
                                    formatted_ts.append((tms_step, Decimal(0)))
                                i += 1
                            df = pd.DataFrame(data=formatted_ts, columns=['time', 'value'])
                            print('get_station_timeseries|df: ', df)
                            return df
                        else:
                            print('Missing data.')
                            return None
                    else:
                        print('No data.')
                        return None
                except Exception as e:
                    print('get_station_timeseries|data fetch|Exception:', e)
                    return None
            else:
                print('No hash id.')
                return None
        except Exception as e:
            print('get_station_timeseries|Exception:', e)
            return None

    def get_timeseries_by_id(self, hash_id, timeseries_start, timeseries_end, time_step_size=5):
        cursor = self.cursor
        data_sql = 'select time,value from curw_sim.data where time>=\'{}\' and time<=\'{}\' and id=\'{}\' '.format(
            timeseries_start, timeseries_end, hash_id)
        try:
            print('data_sql : ', data_sql)
            cursor.execute(data_sql)
            results = cursor.fetchall()
            # print('results : ', results)
            if len(results) > 0:
                time_step_count = int((datetime.strptime(timeseries_end, '%Y-%m-%d %H:%M:%S')
                                       - datetime.strptime(timeseries_start,
                                                           '%Y-%m-%d %H:%M:%S')).total_seconds() / (
                                              60 * time_step_size))
                print('timeseries_start : {}'.format(timeseries_start))
                print('timeseries_end : {}'.format(timeseries_end))
                print('time_step_count : {}'.format(time_step_count))
                print('len(results) : {}'.format(len(results)))
                data_error = ((time_step_count - len(results)) / time_step_count) * 100
                if data_error < 0:
                    df = pd.DataFrame(data=results, columns=['time', 'value'])
                    return df
                elif data_error < 30:
                    print('data_error : {}'.format(data_error))
                    print('filling missing data.')
                    formatted_ts = []
                    i = 0
                    for step in range(time_step_count + 1):
                        tms_step = datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S') + timedelta(
                            minutes=step * time_step_size)
                        # print('tms_step : ', tms_step)
                        if step < len(results):
                            if tms_step == results[i][0]:
                                formatted_ts.append(results[i])
                            else:
                                formatted_ts.append((tms_step, Decimal(0)))
                        else:
                            formatted_ts.append((tms_step, Decimal(0)))
                        i += 1
                    df = pd.DataFrame(data=formatted_ts, columns=['time', 'value'])
                    print('get_station_timeseries|df: ', df)
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

    def get_available_stations(self, date_time, model='hechms', method='MME'):
        available_list = []
        print('get_available_stations|date_time : ', date_time)
        cursor = self.cursor
        try:
            sql = 'select id,grid_id, latitude, longitude from curw_sim.run where model=\'{}\' and method=\'{}\'  and obs_end>=\'{}\''.format(
                model, method, date_time)
            print('sql : ', sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                hash_id = row[0]
                station = row[1].split('_')[2]
                available_list.append([hash_id, station])
        except Exception as e:
            print('get_available_stations|Exception:', e)
        finally:
            return available_list

    def get_available_stations_info(self, date_time, model='hechms', method='MME'):
        """
        To get station information where it has obs_end for before the given limit
        :param date_time: '2019-08-27 05:00:00'
        :param model:
        :param method:
        :return: {station_name:{'hash_id': hash_id, 'latitude': latitude, 'longitude': longitude},
        station_name1:{'hash_id': hash_id1, 'latitude': latitude1, 'longitude': longitude1}}
        """
        available_stations = {}
        print('get_available_stations_info|date_time : ', date_time)
        cursor = self.cursor
        try:
            sql = 'select id, grid_id, latitude, longitude from curw_sim.run where model=\'{}\' and method=\'{}\'  and obs_end>=\'{}\''.format(
                model, method, date_time)
            print('sql : ', sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                hash_id = row[0]
                station = row[1].split('_')[1]
                latitude = Decimal(row[2])
                longitude = Decimal(row[3])
                available_stations[station] = {'hash_id': hash_id, 'latitude': latitude, 'longitude': longitude}
        except Exception as e:
            print('get_available_stations_info|Exception:', e)
        finally:
            return available_stations

    def get_available_stations_in_sub_basin(self, sub_basin_shape_file, date_time):
        """
        Getting station points resides in the given shapefile
        :param db_adapter:
        :param sub_basin_shape_file:
        :param date_time: '2019-08-28 11:00:00'
        :return: {station1:{'hash_id': hash_id1, 'latitude': latitude1, 'longitude': longitude1}, station2:{}}
        """
        available_stations = self.get_available_stations_info(date_time)
        corrected_available_stations = {}
        if len(available_stations):
            for station, info in available_stations.items():
                shape_attribute = ['OBJECTID', 1]
                shape_df = gpd.GeoDataFrame.from_file(sub_basin_shape_file)
                shape_polygon_idx = shape_df.index[shape_df[shape_attribute[0]] == shape_attribute[1]][0]
                shape_polygon = shape_df['geometry'][shape_polygon_idx]
                if Point(info['longitude'], info['latitude']).within(
                        shape_polygon):  # make a point and see if it's in the polygon
                    corrected_available_stations[station] = info
                    print('Station {} in the sub-basin'.format(station))
            return corrected_available_stations
        else:
            print('Not available stations..')
            return {}

    def get_basin_available_stations_timeseries(self, shape_file, start_time, end_time, allowed_error=0.7):
        """
        Add time series to the given available station list.
        :param shape_file:
        :param hourly_csv_file_dir:
        :param adapter:
        :param start_time: '2019-08-28 11:00:00'
        :param end_time: '2019-08-28 11:00:00'
        :return: {station1:{'hash_id': hash_id1, 'latitude': latitude1, 'longitude': longitude1, 'timeseries': timeseries1}, station2:{}}
        """
        basin_available_stations = self.get_available_stations_in_sub_basin(shape_file, start_time)
        print('get_basin_available_stations_timeseries|basin_available_stations: ', basin_available_stations)
        for station in list(basin_available_stations):
            hash_id = basin_available_stations[station]['hash_id']
            station_df = self.get_timeseries_by_id(hash_id, start_time, end_time)
            if station_df is not None:
                if validate_dataframe(station_df, allowed_error):
                    basin_available_stations[station]['timeseries'] = station_df.replace(MISSING_VALUE,
                                                                                         FILL_VALUE)
                else:
                    print('Invalid dataframe station : ', station)
                    basin_available_stations.pop(station, None)
            else:
                print('No times series data avaialble for the station ', station)
                basin_available_stations.pop(station, None)
        return basin_available_stations

    def get_matching_wrf_station_by_grid_id(self, grid_id):
        query = 'select d03_1 from curw_sim.grid_map_obs where grid_id=\'{}\';'.format(grid_id)
        result = self.get_single_result(query)
        if result is not None:
            station_id = result[0]
            return station_id
        return None


class CurwFcstAdapter:
    __instance = None

    @staticmethod
    def get_instance(db_config):
        """ Static access method. """
        print('get_instance|db_config : ', db_config)
        if CurwFcstAdapter.__instance is None:
            CurwFcstAdapter(db_config['mysql_user'], db_config['mysql_password'],
                           db_config['mysql_host'], db_config['mysql_db'],
                           db_config['log_path'])
        return CurwFcstAdapter.__instance

    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db, log_path):
        """ Virtually private constructor. """
        if CurwFcstAdapter.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            try:
                self.connection = mysql.connector.connect(user=mysql_user,
                                                          password=mysql_password,
                                                          host=mysql_host,
                                                          database=mysql_db)
                self.cursor = self.connection.cursor(buffered=True)
                logging.basicConfig(filename=os.path.join(log_path, 'curw_fcst_db_adapter.log'),
                                    level=logging.DEBUG,
                                    format=LOG_FORMAT)
                self.log = logging.getLogger()
                CurwFcstAdapter.__instance = self
            except ConnectionError as ex:
                print('ConnectionError|ex: ', ex)

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    def get_single_result(self, sql_query):
        value = None
        cursor = self.cursor
        try:
            cursor.execute(sql_query)
            result = cursor.fetchone()
            if result:
                value = result
            else:
                self.log.error('no result|query:'.format(sql_query))
        except Exception as ex:
            print('get_single_result|Exception : ', str(ex))
            self.log.error('exception|query:'.format(sql_query))
        finally:
            return value

    def get_multiple_result(self, sql_query):
        cursor = self.cursor
        try:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            if result:
                return result
            else:
                self.log.error('no result|query:'.format(sql_query))
                return None
        except Exception as ex:
            print('get_single_result|Exception : ', str(ex))
            self.log.error('exception|query:'.format(sql_query))
            return None

    def get_station_fcst_rainfall(self, station_ids, fcst_start, fcst_end, source=8, sim_tag='evening_18hrs'):
        """
        :param station_ids: list of station ids
        :param fcst_start:
        :param fcst_end:
        :return:{station_id:dataframe, }
        """
        fcst_ts = {}
        cursor = self.cursor
        station_ids_str = ','.join(station_ids)
        try:
            sql = 'select station as station_id, id as hash_id from curw_fcst.run where sim_tag={} and source={} ' \
                  'and station in ({}) '.format(sim_tag, source, station_ids_str)
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                for row in results:
                    station_id = row[0]
                    hash_id = row[1]
                    try:
                        sql = 'select time,value from curw_fcst.data where time>=\'{}\' and time<\'{}\' and id=\'{}\' '.format(
                            fcst_start, fcst_end, hash_id)
                        cursor.execute(sql)
                        results = cursor.fetchall()
                        if len(results) > 0:
                            fcst_ts[station_id] = pd.DataFrame(data=results, columns=['time', 'value'])
                    except Exception as e:
                        print('Exception:', str(e))
                return fcst_ts
            else:
                return None
        except Exception as e:
            print('save_init_state|Exception:', e)
            return None

    def get_source_id(self, model, version):
        sql_query = 'select id from curw_fcst.source where model=\'{}\' and version=\'{}\';'.format(model, version)
        print('get_source_id|sql_query : ', sql_query)
        result = self.get_single_result(sql_query)
        if result is not None:
            return result[0]
        else:
            return None

    def get_hash_id_of_station(self, variable, unit, source, station_id, sim_tag, exec_date):
        sql_query = 'select id from curw_fcst.run where variable={} and unit={} and source={} ' \
                    'and station=\'{}\' and sim_tag=\'{}\' and end_date >= \'{}\';'.format(variable, unit, source,
                                                                                           station_id,
                                                                                           sim_tag, exec_date)
        print('get_hash_id_of_station|sql_query : ', sql_query)
        result = self.get_single_result(sql_query)
        if result is not None:
            return result[0]
        else:
            return None

    def get_station_tms(self, hash_id, exec_date, tms_start, tms_end):
        sql_query = 'select time,value from curw_fcst.data where id=\'{}\' and ' \
                    'time >= \'{}\' and time <= \'{}\' and ' \
                    'fgt >= \'{}\';'.format(hash_id, tms_start, tms_end, exec_date)
        print('get_station_tms|sql_query : ', sql_query)
        results = self.get_multiple_result(sql_query)
        if results is not None:
            df = pd.DataFrame(data=results, columns=['time', 'value'])
            return df
        return None

    def get_flo2d_cell_map(self, model, version):
        sql_query = 'select parameters from curw_fcst.source where model=\'{}\' and version=\'{}\';'.format(model,
                                                                                                            version)
        print('get_source_id|sql_query : ', sql_query)
        result = self.get_single_result(sql_query)
        if result is not None:
            cell_map = json.loads(result[0])
            return cell_map
        else:
            return None

    def get_flo2d_station_id_by_name(self, name):
        sql_query = 'select id from curw_fcst.station where name=\'{}\';'.format(name)
        print('get_source_id|sql_query : ', sql_query)
        result = self.get_single_result(sql_query)
        if result is not None:
            station_id = result[0]
            return station_id
        else:
            return None


class CurwObsAdapter:
    __instance = None

    @staticmethod
    def get_instance(db_config):
        """ Static access method. """
        print('get_instance|db_config : ', db_config)
        if CurwObsAdapter.__instance is None:
            CurwObsAdapter(db_config['mysql_user'], db_config['mysql_password'],
                           db_config['mysql_host'], db_config['mysql_db'], db_config['log_path'])
        return CurwObsAdapter.__instance

    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db, log_path):
        """ Virtually private constructor. """
        if CurwObsAdapter.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            try:
                self.connection = mysql.connector.connect(user=mysql_user,
                                                          password=mysql_password,
                                                          host=mysql_host,
                                                          database=mysql_db)
                self.cursor = self.connection.cursor(buffered=True)
                logging.basicConfig(filename=os.path.join(log_path, 'curw_fcst_db_adapter.log'),
                                    level=logging.DEBUG,
                                    format=LOG_FORMAT)
                self.log = logging.getLogger()
                CurwObsAdapter.__instance = self
            except ConnectionError as ex:
                print('ConnectionError|ex: ', ex)

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    def get_single_result(self, sql_query):
        value = None
        cursor = self.cursor
        try:
            cursor.execute(sql_query)
            result = cursor.fetchone()
            if result:
                value = result
            else:
                self.log.error('no result|query:'.format(sql_query))
        except Exception as ex:
            print('get_single_result|Exception : ', str(ex))
            self.log.error('exception|query:'.format(sql_query))
        finally:
            return value

    def get_multiple_result(self, sql_query):
        cursor = self.cursor
        try:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            if result:
                return result
            else:
                self.log.error('no result|query:'.format(sql_query))
                return None
        except Exception as ex:
            print('get_single_result|Exception : ', str(ex))
            self.log.error('exception|query:'.format(sql_query))
            return None

    def get_station_id_by_lat_lon(self, lat, lon):
        sql_query = 'select id,name curw_obs.station where latitude={} and longitude={}'.format(lat, lon)
        result = self.get_single_result(sql_query)
        if result is not None:
            return {'id': result[0], 'name': result[1]}
        else:
            return {}

    def get_hash_by_id(self, station_id, start_time):
        sql_query = 'select id from curw_obs.run where station={} and end_date > {}'.format(station_id, start_time)
        result = self.get_single_result(sql_query)
        if result is not None:
            hash_id = result[0]
            return hash_id
        return None

    def get_timeseries_by_id(self, hash_id, timeseries_start, timeseries_end, time_step_size=5):
        cursor = self.cursor
        data_sql = 'select time,value from curw_obs.data where time>=\'{}\' and time<=\'{}\' and id=\'{}\' '.format(
            timeseries_start, timeseries_end, hash_id)
        try:
            print('data_sql : ', data_sql)
            cursor.execute(data_sql)
            results = cursor.fetchall()
            # print('results : ', results)
            if len(results) > 0:
                time_step_count = int((datetime.strptime(timeseries_end, '%Y-%m-%d %H:%M:%S')
                                       - datetime.strptime(timeseries_start,
                                                           '%Y-%m-%d %H:%M:%S')).total_seconds() / (
                                              60 * time_step_size))
                print('timeseries_start : {}'.format(timeseries_start))
                print('timeseries_end : {}'.format(timeseries_end))
                print('time_step_count : {}'.format(time_step_count))
                print('len(results) : {}'.format(len(results)))
                data_error = ((time_step_count - len(results)) / time_step_count) * 100
                if data_error < 0:
                    df = pd.DataFrame(data=results, columns=['time', 'value'])
                    return df
                elif data_error < 30:
                    print('data_error : {}'.format(data_error))
                    print('filling missing data.')
                    formatted_ts = []
                    i = 0
                    for step in range(time_step_count + 1):
                        tms_step = datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S') + timedelta(
                            minutes=step * time_step_size)
                        # print('tms_step : ', tms_step)
                        if step < len(results):
                            if tms_step == results[i][0]:
                                formatted_ts.append(results[i])
                            else:
                                formatted_ts.append((tms_step, Decimal(0)))
                        else:
                            formatted_ts.append((tms_step, Decimal(0)))
                        i += 1
                    df = pd.DataFrame(data=formatted_ts, columns=['time', 'value'])
                    print('get_station_timeseries|df: ', df)
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

    def get_data_by_hash(self, hash_id, start_time, end_time):
        data_sql = 'select time,value from curw_obs.data where time>=\'{}\' and time<=\'{}\' and id=\'{}\' '.format(
            start_time, end_time, hash_id)
        results = self.get_multiple_result(data_sql)
        if results is not None:
            df = pd.DataFrame(data=results, columns=['time', 'value'])

    def get_ts_df(self, lat, lon, start_time, end_time):
        print('get_ts_df|[lat, lon, start_time, end_time] : ', lat, lon, start_time, end_time)
        station_meta = self.get_station_id_by_lat_lon(lat, lon)
        if len(station_meta.keys()) > 0:
            station_id = station_meta['id']
            station_name = station_meta['name']
            hash_id = self.get_hash_by_id(station_id, start_time)
            if hash_id is not None:
                ts_df = self.get_timeseries_by_id(hash_id, start_time, end_time)

    def get_station_id_by_name(self, station_type, station_name):
        sql_query = 'select id,latitude,longitude from curw_obs.station where ' \
                    'station_type = \'{}\' and name = \'{}\''.format(station_type, station_name)
        print('get_station_id_by_name|sql_query : ', sql_query)
        result = self.get_single_result(sql_query)
        if result is not None:
            station_id = result[0]
            return station_id
        return None

    def get_station_hash_id(self, station_id, variable, unit):
        sql_query = 'select id from curw_obs.run where station=\'{}\' and ' \
                    'variable=\'{}\' and unit=\'{}\';'.format(station_id, variable, unit)
        print('get_station_hash_id|sql_query : ', sql_query)
        result = self.get_single_result(sql_query)
        if result is not None:
            hash_id = result[0]
            return hash_id
        return None

    def get_station_ids_for_location(self, locations, variable_type):
        location_ids = []
        for location in locations:
            sql_query = 'select id from curw_obs.station where name=\'{}\' and station_type=\'{}\';'.format(location,
                                                                                                            variable_type)
            result = self.get_single_result(sql_query)
            if result is not None:
                location_id = result[0]
                location_ids.append({'location': location, 'id': location_id})
        return location_ids

    def get_location_hash_ids(self, location_ids):
        location_hash_ids = []
        for location_id in location_ids:
            if location_id['id']:
                sql_query = 'select id from curw_obs.run where station={} and variable=10;'.format(location_id['id'])
                result = self.get_single_result(sql_query)
                if result is not None:
                    location_id['hash_id'] = result[0]
                    location_hash_ids.append(location_id)
        return location_hash_ids

    def get_values_for_hash_ids(self, location_hash_ids):
        variable_values = []
        for location_hash_id in location_hash_ids:
            if location_hash_id['hash_id']:
                sql_query = 'select time,value from curw_obs.data where id=\'{}\' order by time desc limit 1;'.format(
                    location_hash_id['hash_id'])
                result = self.get_single_result(sql_query)
                if result is not None:
                    location_hash_id['time'] = result[0]
                    location_hash_id['value'] = result[1]
                    variable_values.append(location_hash_id)
        return variable_values

    def get_current_rainfall_for_given_location_set(self, locations, variable_type):
        location_ids = self.get_station_ids_for_location(locations, variable_type)
        if len(location_ids) > 0:
            location_hash_ids = self.get_location_hash_ids(location_ids)
            if len(location_hash_ids) > 0:
                variable_values = self.get_values_for_hash_ids(location_hash_ids)
                if len(variable_values):
                    return variable_values
        return None

