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

    def get_wrf_rule_info(self):
        print('')

    def get_hechms_rule_info(self):
        print('')

    def get_flo2d_rule_info(self):
        print('')


if __name__ == "__main__":
    adapter = RuleEngineAdapter('admin',
                                'floody',
                                'localhost',
                                'dss',
                                '/home/hasitha/PycharmProjects/DSS-Framework/log')
    sql = 'SELECT * FROM dss.wrf_rules'
    adapter.get_multiple_result(sql)
    adapter.close_connection()

