import mysql.connector


class RuleEngineAdapter:
    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db):
        print('[mysql_user, mysql_password, mysql_host, mysql_db] : ',
              [mysql_user, mysql_password, mysql_host, mysql_db])
        try:
            self.connection = mysql.connector.connect(user=mysql_user,
                                                      password=mysql_password,
                                                      host=mysql_host,
                                                      database=mysql_db)
            self.cursor = self.connection.cursor(buffered=True)
        except ConnectionError as ex:
            print('ConnectionError|ex: ', ex)

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

