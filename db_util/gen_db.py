import logging
import os

import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal

LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'


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
                            df = pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
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
                            df = pd.DataFrame(data=formatted_ts, columns=['time', 'value']).set_index(keys='time')
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
                if data_error < 1:
                    df = pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
                    return df
                else:
                    print('data_error : {}'.format(data_error))
                    print('filling missing data.')
                    formatted_ts = []
                    i = 0
                    for step in range(time_step_count):
                        tms_step = datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S') + timedelta(
                            minutes=step * time_step_size)
                        if step < len(results):
                            if tms_step == results[i][0]:
                                formatted_ts.append(results[i])
                            else:
                                formatted_ts.append((tms_step, Decimal(0)))
                        else:
                            formatted_ts.append((tms_step, Decimal(0)))
                        i += 1
                    df = pd.DataFrame(data=formatted_ts, columns=['time', 'value']).set_index(keys='time')
                    print('get_station_timeseries|df: ', df)
                    return df
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
    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db):
        print('[mysql_user, mysql_password, mysql_host, mysql_db] : ',
              [mysql_user, mysql_password, mysql_host, mysql_db])
        try:
            self.connection = mysql.connector.connect(user=mysql_user,
                                                      password=mysql_password,
                                                      host=mysql_host,
                                                      database=mysql_db)
            self.cursor = self.connection.cursor()
        except ConnectionError as ex:
            print('ConnectionError|ex: ', ex)

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

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

