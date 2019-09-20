import logging
import mysql.connector
import os

LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'


class RuleEngineAdapter:
    __instance = None

    @staticmethod
    def get_instance(mysql_user, mysql_password, mysql_host, mysql_db, log_path):
        """ Static access method. """
        if RuleEngineAdapter.__instance is None:
            RuleEngineAdapter(mysql_user, mysql_password, mysql_host, mysql_db, log_path)
        return RuleEngineAdapter.__instance

    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db, log_path):
        """ Virtually private constructor. """
        if RuleEngineAdapter.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            try:
                self.connection = mysql.connector.connect(user=mysql_user,
                                                          password=mysql_password,
                                                          host=mysql_host,
                                                          database=mysql_db)
                self.cursor = self.connection.cursor(buffered=True)
                logging.basicConfig(filename=os.path.join(log_path, 'rule_engine_db_adapter.log'),
                                    level=logging.DEBUG,
                                    format=LOG_FORMAT)
                self.log = logging.getLogger()
                RuleEngineAdapter.__instance = self
            except ConnectionError as ex:
                print('ConnectionError|ex: ', ex)


if __name__ == "__main__":
    adapter = RuleEngineAdapter('admin',
                                'floody',
                                'localhost',
                                'dss',
                                '/home/hasitha/PycharmProjects/DSS-Framework/log')
    print(adapter)
    adapter = RuleEngineAdapter.get_instance('admin',
                                   'floody',
                                   'localhost',
                                   'dss',
                                   '/home/hasitha/PycharmProjects/DSS-Framework/log')
    print(adapter)
    adapter = RuleEngineAdapter.get_instance('admin',
                                             'floody',
                                             'localhost',
                                             'dss',
                                             '/home/hasitha/PycharmProjects/DSS-Framework/log')
    print(adapter)




