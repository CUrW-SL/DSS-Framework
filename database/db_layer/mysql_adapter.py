import logging
import mysql.connector
import os


class RuleEngineAdapter:
    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db, log_path):
        print('[mysql_user, mysql_password, mysql_host, mysql_db, log_path] : ',
              [mysql_user, mysql_password, mysql_host, mysql_db, log_path])
        try:
            self.connection = mysql.connector.connect(user=mysql_user,
                                                      password=mysql_password,
                                                      host=mysql_host,
                                                      database=mysql_db)
            self.cursor = self.connection.cursor(buffered=True)
            LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
            logging.basicConfig(filename=os.path.join(log_path, 'mysql_adapter.log'),
                                level=logging.DEBUG,
                                format=LOG_FORMAT)
            self.log = logging.getLogger()
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
                value = result[0]
            else:
                self.log.error('no result|query:'.format(sql_query))
        except Exception as ex:
            print('get_single_result|Exception : ', str(ex))
            self.log.error('exception|query:'.format(sql_query))
        finally:
            return value

    def get_multiple_result(self, sql_query):
        result = None
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

    def get_wrf_rule_info(self, status=1):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rules = []
        query = 'select name, target_model, version, run, data_hour, ignore_previous_run, ' \
              'check_gfs_data_availability from dss.wrf_rules ' \
              'where status = {} order by priority asc'.format(status)
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                wrf_rules.append({'name': row[0], 'target_model': row[1],
                                  'version': row[2], 'run': row[3],
                                  'data_hour': row[4], 'ignore_previous_run': row[5],
                                  'check_gfs_data_availability': row[6]})
        return wrf_rules

    def get_hechms_rule_info(self, status=1):
        hechms_rules = []
        query = 'select name, target_model,forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rainfall_data_from, ' \
                'ignore_previous_run from dss.hechms_rules where status = 1'.format(status)
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                hechms_rules.append({'name': row[0], 'target_model': row[1],
                                  'forecast_days': row[2], 'observed_days': row[3],
                                  'init_run': row[4], 'no_forecast_continue': row[5],
                                  'no_observed_continue': row[6], 'rainfall_data_from': row[7],
                                  'ignore_previous_run': row[8]})
        return hechms_rules

    def get_flo2d_rule_info(self, status=1):
        flo2d_rules = []
        query = 'select name, target_model, forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rain_cell_data_from, ' \
                'inflow_data_from, outflow_data_from, ignore_previous_run ' \
                'from dss.flo2d_rules where status=1'.format(status)
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                flo2d_rules.append({'name': row[0], 'target_model': row[1],
                                    'forecast_days': row[2], 'observed_days': row[3],
                                    'init_run': row[4], 'no_forecast_continue': row[5],
                                    'no_observed_continue': row[6], 'rain_cell_data_from': row[7],
                                    'inflow_data_from': row[8], 'outflow_data_from': row[9],
                                    'ignore_previous_run': row[10]})
        return flo2d_rules


if __name__ == "__main__":
    adapter = RuleEngineAdapter('admin',
                                'floody',
                                'localhost',
                                'dss',
                                '/home/hasitha/PycharmProjects/DSS-Framework/log')
    sql = 'SELECT * FROM dss.wrf_rules'
    adapter.get_multiple_result(sql)
    print(adapter.get_wrf_rule_info(1))
    print(adapter.get_hechms_rule_info(1))
    print(adapter.get_flo2d_rule_info(1))
    adapter.close_connection()

