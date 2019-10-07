import logging
import mysql.connector
import os
from datetime import datetime
import croniter

LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'


def get_next_scheduled_workflow(schedule_date, workflow_routines):
    for workflow_routine in workflow_routines:
        if validate_workflow(workflow_routine, schedule_date):
            return workflow_routine
    return None


def validate_workflow(workflow_routine, schedule_date):
    schedule = workflow_routine['schedule']
    cron = croniter.croniter(schedule, schedule_date)
    run_date = cron.get_next(datetime.datetime)
    if datetime.now() >= run_date:
        return True
    else:
        return False


class RuleEngineAdapter:
    __instance = None

    @staticmethod
    def get_instance(db_config):
        """ Static access method. """
        print('get_instance|db_config : ', db_config)
        if RuleEngineAdapter.__instance is None:
            RuleEngineAdapter(db_config['mysql_user'], db_config['mysql_password'],
                              db_config['mysql_host'], db_config['mysql_db'],
                              db_config['log_path'])
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
        query = 'select name, target_model, version, run, hour, ignore_previous_run, ' \
                'check_gfs_data_availability from dss.wrf_rules ' \
                'where status = {} '.format(status)
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                wrf_rules.append({'name': row[0], 'target_model': row[1],
                                  'version': row[2], 'run': row[3],
                                  'hour': row[4], 'ignore_previous_run': row[5],
                                  'check_gfs_data_availability': row[6]})
        return wrf_rules

    def get_hechms_rule_info(self, status=1):
        hechms_rules = []
        query = 'select name, target_model,forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rainfall_data_from, ' \
                'ignore_previous_run from dss.hechms_rules where status = {}'.format(status)
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
                'init_run, no_forecast_continue, no_observed_continue, raincell_data_from, ' \
                'inflow_data_from, outflow_data_from, ignore_previous_run' \
                'from dss.flo2d_rules where status=1'.format(status)
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                flo2d_rules.append({'name': row[0], 'target_model': row[1],
                                    'forecast_days': row[2], 'observed_days': row[3],
                                    'init_run': row[4], 'no_forecast_continue': row[5],
                                    'no_observed_continue': row[6], 'raincell_data_from': row[7],
                                    'inflow_data_from': row[8], 'outflow_data_from': row[9],
                                    'ignore_previous_run': row[10]})
        return flo2d_rules

    def get_workflow_routines(self, schedule_date, status):
        workflow_routines = []
        query = 'select id,dss1,dss2,dss3 from dss.workflow_routines where scheduled_date>=\'{}\' ' \
                'and status={} order by priority asc '.format(schedule_date, status)
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                workflow_routines.append({'id': row[0], 'dss1': row[1], 'dss2': row[2], 'dss3': row[3]})
        return workflow_routines

    def update_query(self, query):
        cursor = self.cursor
        try:
            print(query)
            cursor.execute(query)
            self.connection.commit()
        except Exception as ex:
            print('update_rule_status|Exception: ', str(ex))
            self.log.error('update_rule_status|query:{}'.format(query))

    def update_rule_status(self, model, rule_name, status):
        if model == 'wrf':
            query = 'update dss.wrf_rules set status={} where name=\'{}\''.format(status, rule_name)
        elif model == 'hechms':
            query = 'update dss.hechms_rules set status={} where name=\'{}\''.format(status, rule_name)
        elif model == 'flo2d':
            query = 'update dss.flo2d_rules set status={} where name=\'{}\''.format(status, rule_name)
        self.update_query(query)

    def update_workflow_routing_status(self, status, routine_id):
        query = 'update dss.workflow_routines set status={} where name=\'{}\''.format(status, routine_id)
        self.update_query(query)

    def update_initial_workflow_routing_status(self, status, routine_id):
        query = 'update dss.workflow_routines set status={},last_trigger_date=now()  ' \
                'where id=\'{}\''.format(status, routine_id)
        self.update_query(query)

    def get_next_workflow_routine(self, schedule_date=None):
        if schedule_date is None:
            query = 'select id,dss1,dss2,dss3 from dss.workflow_routines where status=0 and ' \
                    'scheduled_date<=now() order by priority asc limit 1;'
        else:
            query = 'select id,dss1,dss2,dss3 from dss.workflow_routines where status=0 and ' \
                    'scheduled_date<=\'{}\' order by priority asc limit 1;'.format(schedule_date)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            print(result)
            routine = {'id': result[0], 'dss1': result[1], 'dss2': result[2], 'dss3': result[3]}
            self.update_initial_workflow_routing_status(1, routine['id'])
            return routine

    def get_next_workflow_routines(self, schedule_date=datetime.now()):
        if type(schedule_date) is datetime:
            schedule_date = schedule_date
        else:
            schedule_date = datetime.strptime(schedule_date, '%Y-%m-%d %H:%M:%S')
        query = 'select id,dss1,dss2,dss3,schedule from dss.workflow_routines where status=0 and ;'
        results = self.get_multiple_result(query)
        routines = []
        if results is not None:
            for result in results:
                print(result)
                routines.append({'id': result[0], 'dss1': result[1], 'dss2': result[2],
                                 'dss3': result[3], 'schedule': result[3]})


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




