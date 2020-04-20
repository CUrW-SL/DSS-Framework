import logging
import mysql.connector
import os
from datetime import datetime
import croniter
import json

LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'


def get_next_scheduled_routines(schedule_date, workflow_routines):
    scheduled_list = []
    print('get_next_scheduled_workflow|schedule_date : ', schedule_date)
    print('get_next_scheduled_workflow|workflow_routines : ', workflow_routines)
    for workflow_routine in workflow_routines:
        if validate_workflow(workflow_routine, schedule_date):
            print('get_next_scheduled_workflow|validate_workflow|workflow_routine : ', workflow_routine)
            scheduled_list.append(workflow_routine)
    return scheduled_list


def get_next_scheduled_workflow(schedule_date, workflow_routines):
    print('get_next_scheduled_workflow|schedule_date : ', schedule_date)
    print('get_next_scheduled_workflow|workflow_routines : ', workflow_routines)
    for workflow_routine in workflow_routines:
        if validate_workflow(workflow_routine, schedule_date):
            print('get_next_scheduled_workflow|validate_workflow|workflow_routine : ', workflow_routine)
            return workflow_routine
    return None


def validate_workflow(workflow_routine, schedule_date):
    schedule = workflow_routine['schedule']
    print('validate_workflow|workflow_routine : ', workflow_routine)
    print('validate_workflow|[schedule, schedule_date] : ', [schedule, schedule_date])
    cron = croniter.croniter(schedule, schedule_date)
    run_date = cron.get_next(datetime)
    print('validate_workflow|run_date : ', run_date)
    # current_date = datetime.now()
    current_date = datetime.utcnow()
    print('validate_workflow|current_date : ', current_date)
    if current_date >= run_date:
        print('validate_workflow|True')
        return True
    else:
        print('validate_workflow|False')
        return False


class RuleEngineAdapter:
    __instance = None

    @staticmethod
    def get_instance(db_config):
        """ Static access method. """
        # print('get_instance|db_config : ', db_config)
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

    def get_single_row(self, sql_query):
        cursor = self.cursor
        try:
            cursor.execute(sql_query)
            result = cursor.fetchone()
            if result:
                return result
            else:
                self.log.error('no result|query:'.format(sql_query))
                return None
        except Exception as ex:
            print('get_single_result|Exception : ', str(ex))
            self.log.error('exception|query:'.format(sql_query))
            return None

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
            print('get_multiple_result|Exception : ', str(ex))
            self.log.error('exception|query:'.format(sql_query))
            return None

    # -------------------------WRF rule information-------------------------
    def get_wrf_rule_info(self, status=1):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rules = []
        query = 'select id, name, target_model, version, run, hour, ignore_previous_run, ' \
                'check_gfs_data_availability, namelist_wps, namelist_input from dss.wrf_rules ' \
                'where status = {} '.format(status)
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                wrf_rules.append({'id': row[0], 'name': row[1], 'target_model': row[2],
                                  'version': row[3], 'run': row[4], 'hour': row[5],
                                  'ignore_previous_run': row[6], 'check_gfs_data_availability': row[7],
                                  'namelist_wps': row[8], 'namelist_input': row[9]})
        return wrf_rules

    def get_wrf_rule_info_by_id(self, rule_id):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rule = None
        query = 'select id, name, target_model, version, run, hour, ignore_previous_run, ' \
                'check_gfs_data_availability,accuracy_rule, rule_details, namelist_wps, ' \
                'namelist_input, timeout from dss.wrf_rules ' \
                'where id = {}'.format(rule_id)
        print('get_wrf_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            wrf_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                        'version': result[3], 'run': result[4], 'hour': result[5],
                        'ignore_previous_run': result[6], 'check_gfs_data_availability': result[7],
                        'accuracy_rule': result[8], 'rule_details': json.loads(result[9]),
                        'namelist_wps': result[10], 'namelist_input': result[11],
                        'timeout': json.loads(result[12])}
        return wrf_rule

    def get_eligible_wrf_rule_info_by_id(self, rule_id):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rule = None
        query = 'select id, name, target_model, version, run, hour, ignore_previous_run, ' \
                'check_gfs_data_availability,accuracy_rule, rule_details, namelist_wps, ' \
                'namelist_input from dss.wrf_rules ' \
                'where id = {} and status in (1,3,4,5)  '.format(rule_id)
        print('get_wrf_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            wrf_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                        'version': result[3], 'run': result[4], 'hour': result[5],
                        'ignore_previous_run': result[6], 'check_gfs_data_availability': result[7],
                        'accuracy_rule': result[8], 'rule_details': json.loads(result[9]),
                        'namelist_wps': result[10], 'namelist_input': result[11]}
        return wrf_rule

    def get_wrf_rule_status_by_id(self, rule_id):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rule = None
        query = 'select id, status from dss.wrf_rules where id = {}'.format(rule_id)
        print('get_wrf_rule_status_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        print('get_wrf_rule_status_by_id|result : ', result)
        if result is not None:
            wrf_rule = {'id': result[0], 'status': result[1]}
            print('get_wrf_rule_status_by_id|wrf_rule : ', wrf_rule)
        return wrf_rule

    def update_wrf_rule_status(self, status, rule_id):
        query = 'update `dss`.`wrf_rules` set `status`=\'{}\' ' \
                'WHERE `id`=\'{}\';'.format(status, rule_id)
        print('update_wrf_rule_status|query : ', query)
        self.update_query(query)

    def get_eligible_wrf_rules(self):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rules = []
        query = 'select id, name, target_model, version, run, hour, ignore_previous_run, ' \
                'check_gfs_data_availability,accuracy_rule, rule_details, namelist_wps, ' \
                'namelist_input, timeout from dss.wrf_rules ' \
                'where status in (1,3,4,5)  '
        print('get_wrf_rule_info_by_id|query : ', query)
        results = self.get_multiple_result(query)
        if results is not None:
            for result in results:
                wrf_rules.append({'id': result[0], 'name': result[1], 'target_model': result[2],
                                  'version': result[3], 'run': result[4], 'hour': result[5],
                                  'ignore_previous_run': result[6], 'check_gfs_data_availability': result[7],
                                  'accuracy_rule': result[8], 'rule_details': json.loads(result[9]),
                                  'namelist_wps': result[10], 'namelist_input': result[11],
                                  'timeout': json.loads(result[12])})
        return wrf_rules

    def get_all_wrf_rules(self):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rules = []
        query = 'select id, name, target_model, version, run, hour, ignore_previous_run, ' \
                'check_gfs_data_availability,accuracy_rule, rule_details, namelist_wps, ' \
                'namelist_input, timeout from dss.wrf_rules ' \
                'where status in (1,2, 3,4,5)  '
        print('get_wrf_rule_info_by_id|query : ', query)
        results = self.get_multiple_result(query)
        if results is not None:
            for result in results:
                wrf_rules.append({'id': result[0], 'name': result[1], 'target_model': result[2],
                                  'version': result[3], 'run': result[4], 'hour': result[5],
                                  'ignore_previous_run': result[6], 'check_gfs_data_availability': result[7],
                                  'accuracy_rule': result[8], 'rule_details': json.loads(result[9]),
                                  'namelist_wps': result[10], 'namelist_input': result[11],
                                  'timeout': json.loads(result[12])})
        return wrf_rules

    def get_wrf_rule_info_by_name(self, rule_name):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rule = None
        query = 'select id, name, target_model, version, run, hour, ignore_previous_run, ' \
                'check_gfs_data_availability,accuracy_rule, rule_details, namelist_wps, ' \
                'namelist_input from dss.wrf_rules ' \
                'where name = \'{}\' and status=3  '.format(rule_name)
        print('get_wrf_rule_info_by_name|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            wrf_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                        'version': result[3], 'run': result[4], 'hour': result[5],
                        'ignore_previous_run': result[6], 'check_gfs_data_availability': result[7],
                        'accuracy_rule': result[8], 'rule_details': json.loads(result[9]),
                        'namelist_wps': result[10], 'namelist_input': result[11]}
        return wrf_rule

    def get_wrf_rule_names(self):
        rule_names = []
        query = 'select name from dss.wrf_rules'
        results = self.get_multiple_result(query)
        if results is not None:
            for result in results:
                rule_names.append(result[0])
        return rule_names

    # -------------------------HecHms rule information-------------------------
    def get_hechms_rule_info(self, status=1):
        hechms_rules = []
        query = 'select id, name, target_model,forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rainfall_data_from, ' \
                'ignore_previous_run from dss.hechms_rules where status = {}'.format(status)
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                hechms_rules.append({'id': row[0], 'name': row[1], 'target_model': row[2],
                                     'forecast_days': row[3], 'observed_days': row[4],
                                     'init_run': row[5], 'no_forecast_continue': row[6],
                                     'no_observed_continue': row[7], 'rainfall_data_from': row[8],
                                     'ignore_previous_run': row[9]})
        return hechms_rules

    def get_hechms_rule_info_by_id(self, id):
        hechms_rule = None
        query = 'select id, name, target_model,forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rainfall_data_from, ' \
                'ignore_previous_run, accuracy_rule, rule_details from dss.hechms_rules where id = {}'.format(id)
        print('get_hechms_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            hechms_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                           'forecast_days': result[3], 'observed_days': result[4],
                           'init_run': result[5], 'no_forecast_continue': result[6],
                           'no_observed_continue': result[7], 'rainfall_data_from': result[8],
                           'ignore_previous_run': result[9], 'accuracy_rule': result[10],
                           'rule_details': json.loads(result[11])}
        return hechms_rule

    def get_eligible_hechms_rule_info_by_id(self, id):
        hechms_rule = None
        query = 'select id, name, target_model,forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rainfall_data_from, ' \
                'ignore_previous_run, accuracy_rule, rule_details from dss.hechms_rules where ' \
                'status in (1, 3, 4, 5) and id = {}'.format(id)
        print('get_hechms_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            hechms_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                           'forecast_days': result[3], 'observed_days': result[4],
                           'init_run': result[5], 'no_forecast_continue': result[6],
                           'no_observed_continue': result[7], 'rainfall_data_from': result[8],
                           'ignore_previous_run': result[9], 'accuracy_rule': result[10],
                           'rule_details': json.loads(result[11])}
        return hechms_rule

    def get_hechms_rule_status_by_id(self, id):
        hechms_rule = None
        query = 'select id, status from dss.hechms_rules where id = {}'.format(id)
        print('get_hechms_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            hechms_rule = {'id': result[0], 'status': result[1]}
        return hechms_rule

    def update_hechms_rule_status(self, rule_id, status):
        query = 'update `dss`.`hechms_rules` set `status`=\'{}\' ' \
                'WHERE `id`=\'{}\';'.format(status, rule_id)
        print('update_hechms_rule_status|query : ', query)
        self.update_query(query)

    def get_eligible_hechms_rules(self):
        hechms_rules = []
        query = 'select id, name, target_model,forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rainfall_data_from, ' \
                'ignore_previous_run, timeout from dss.hechms_rules where status in (1, 3, 4, 5)'
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                hechms_rules.append({'id': row[0], 'name': row[1], 'target_model': row[2],
                                     'forecast_days': row[3], 'observed_days': row[4],
                                     'init_run': row[5], 'no_forecast_continue': row[6],
                                     'no_observed_continue': row[7], 'rainfall_data_from': row[8],
                                     'ignore_previous_run': row[9], 'timeout': json.loads(row[10])})
        return hechms_rules

    def get_all_hechms_rules(self):
        hechms_rules = []
        query = 'select id, name, target_model,forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rainfall_data_from, ' \
                'ignore_previous_run, timeout from dss.hechms_rules where status in (1, 2, 3, 4, 5)'
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                hechms_rules.append({'id': row[0], 'name': row[1], 'target_model': row[2],
                                     'forecast_days': row[3], 'observed_days': row[4],
                                     'init_run': row[5], 'no_forecast_continue': row[6],
                                     'no_observed_continue': row[7], 'rainfall_data_from': row[8],
                                     'ignore_previous_run': row[9], 'timeout': json.loads(row[10])})
        return hechms_rules

    def get_hechms_rule_info_by_name(self, rule_name):
        hechms_rule = None
        query = 'select id, name, target_model,forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rainfall_data_from, ' \
                'ignore_previous_run, accuracy_rule, rule_details from dss.hechms_rules where ' \
                'status=3 and name = \'{}\''.format(rule_name)
        print('get_hechms_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            hechms_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                           'forecast_days': result[3], 'observed_days': result[4],
                           'init_run': result[5], 'no_forecast_continue': result[6],
                           'no_observed_continue': result[7], 'rainfall_data_from': result[8],
                           'ignore_previous_run': result[9], 'accuracy_rule': result[10],
                           'rule_details': json.loads(result[11])}
        return hechms_rule

    # -------------------------Flo2d rule information-------------------------
    def get_flo2d_rule_info(self, status=1):
        flo2d_rules = []
        query = 'select id, name, target_model, forecast_days, observed_days, ' \
                'no_forecast_continue, no_observed_continue, raincell_data_from, ' \
                'inflow_data_from, outflow_data_from, ignore_previous_run ' \
                'from dss.flo2d_rules where status=1'.format(status)
        results = self.get_multiple_result(query)
        if results is not None:
            for result in results:
                flo2d_rules.append({'id': result[0], 'name': result[1], 'target_model': result[2],
                                    'forecast_days': result[3], 'observed_days': result[4],
                                    'no_forecast_continue': result[5], 'no_observed_continue': result[6],
                                    'raincell_data_from': result[7], 'inflow_data_from': result[8],
                                    'outflow_data_from': result[9], 'ignore_previous_run': result[10]})
        return flo2d_rules

    def get_flo2d_rule_info_by_id(self, id):
        flo2d_rule = None
        query = 'select id, name, target_model, forecast_days, observed_days, ' \
                'no_forecast_continue, no_observed_continue, raincell_data_from, ' \
                'inflow_data_from, outflow_data_from, ignore_previous_run, accuracy_rule,' \
                'rule_details ' \
                'from dss.flo2d_rules where id={}'.format(id)
        print('get_flo2d_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            flo2d_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                          'forecast_days': result[3], 'observed_days': result[4],
                          'no_forecast_continue': result[5], 'no_observed_continue': result[6],
                          'raincell_data_from': result[7], 'inflow_data_from': result[8],
                          'outflow_data_from': result[9], 'ignore_previous_run': result[10],
                          'accuracy_rule': result[11], 'rule_details': json.loads(result[12])}
        return flo2d_rule

    def get_eligible_flo2d_rule_info_by_id(self, id):
        flo2d_rule = None
        query = 'select id, name, target_model, forecast_days, observed_days, ' \
                'no_forecast_continue, no_observed_continue, raincell_data_from, ' \
                'inflow_data_from, outflow_data_from, ignore_previous_run, accuracy_rule,' \
                'rule_details ' \
                'from dss.flo2d_rules where status in (1, 3, 4, 5) and id={}'.format(id)
        print('get_eligible_flo2d_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            flo2d_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                          'forecast_days': result[3], 'observed_days': result[4],
                          'no_forecast_continue': result[5], 'no_observed_continue': result[6],
                          'raincell_data_from': result[7], 'inflow_data_from': result[8],
                          'outflow_data_from': result[9], 'ignore_previous_run': result[10],
                          'accuracy_rule': result[11], 'rule_details': json.loads(result[12])}
        return flo2d_rule

    def get_flo2d_rule_status_by_id(self, id):
        flo2d_rule = None
        query = 'select id, status from dss.flo2d_rules where id={}'.format(id)
        print('get_flo2d_rule_status_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            flo2d_rule = {'id': result[0], 'status': result[1]}
        return flo2d_rule

    def update_flo2d_rule_status(self, rule_id, status):
        query = 'update `dss`.`flo2d_rules` set `status`=\'{}\' ' \
                'WHERE `id`=\'{}\';'.format(rule_id, status)
        print('update_flo2d_rule_status|query : ', query)
        self.update_query(query)

    def get_eligible_flo2d_rules(self):
        flo2d_rules = []
        query = 'select id, name, target_model, forecast_days, observed_days, ' \
                'no_forecast_continue, no_observed_continue, raincell_data_from, ' \
                'inflow_data_from, outflow_data_from, ignore_previous_run, timeout ' \
                'from dss.flo2d_rules where status in (1, 3, 4, 5) '
        results = self.get_multiple_result(query)
        if results is not None:
            for result in results:
                flo2d_rules.append({'id': result[0], 'name': result[1], 'target_model': result[2],
                                    'forecast_days': result[3], 'observed_days': result[4],
                                    'no_forecast_continue': result[5], 'no_observed_continue': result[6],
                                    'raincell_data_from': result[7], 'inflow_data_from': result[8],
                                    'outflow_data_from': result[9], 'ignore_previous_run': result[10],
                                    'timeout': json.loads(result[11])})
        return flo2d_rules

    def get_all_flo2d_rules(self):
        flo2d_rules = []
        query = 'select id, name, target_model, forecast_days, observed_days, ' \
                'no_forecast_continue, no_observed_continue, raincell_data_from, ' \
                'inflow_data_from, outflow_data_from, ignore_previous_run, timeout ' \
                'from dss.flo2d_rules where status in (1,2, 3, 4, 5) '
        results = self.get_multiple_result(query)
        if results is not None:
            for result in results:
                flo2d_rules.append({'id': result[0], 'name': result[1], 'target_model': result[2],
                                    'forecast_days': result[3], 'observed_days': result[4],
                                    'no_forecast_continue': result[5], 'no_observed_continue': result[6],
                                    'raincell_data_from': result[7], 'inflow_data_from': result[8],
                                    'outflow_data_from': result[9], 'ignore_previous_run': result[10],
                                    'timeout': json.loads(result[11])})
        return flo2d_rules

    # ---------------------------Workflow routine-----------------------------
    def get_workflow_routines(self, status):
        workflow_routines = []
        query = 'select id,dss1,dss2,dss3,cascade_on from dss.workflow_routines where status={}'.format(status)
        print('get_workflow_routines|query : ', query)
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                workflow_routines.append({'id': row[0], 'dss1': row[1], 'dss2': row[2], 'dss3': row[3],
                                          'cascade_on': row[4]})
        return workflow_routines

    def get_workflow_routine_info(self, routine_id):
        workflow_routine = None
        query = 'select id,dss1,dss2,dss3,cascade_on from dss.workflow_routines where id={}'.format(routine_id)
        print('get_workflow_routine_info|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        print('get_workflow_routine_info|result : ', result)
        if result is not None:
            workflow_routine = {'id': result[0], 'dss1': result[1], 'dss2': result[2], 'dss3': result[3],
                                'cascade_on': result[4]}
        return workflow_routine

    def update_query(self, query):
        cursor = self.cursor
        try:
            print(query)
            cursor.execute(query)
            self.connection.commit()
        except Exception as ex:
            print('update_rule_status|Exception: ', str(ex))
            self.log.error('update_rule_status|query:{}'.format(query))

    def update_rule_status_by_id(self, model, rule_id, status):
        if model == 'wrf':
            query = 'update dss.wrf_rules set status={} where id=\'{}\''.format(status, rule_id)
        elif model == 'hechms':
            query = 'update dss.hechms_rules set status={} where id=\'{}\''.format(status, rule_id)
        elif model == 'flo2d':
            query = 'update dss.flo2d_rules set status={} where id=\'{}\''.format(status, rule_id)
        print('update_rule_status_by_id|query : ', query)
        self.update_query(query)

    def update_rule_status(self, model, rule_name, status):
        if model == 'wrf':
            query = 'update dss.wrf_rules set status={} where name=\'{}\''.format(status, rule_name)
        elif model == 'hechms':
            query = 'update dss.hechms_rules set status={} where name=\'{}\''.format(status, rule_name)
        elif model == 'flo2d':
            query = 'update dss.flo2d_rules set status={} where name=\'{}\''.format(status, rule_name)
        print('update_rule_status|query : ', query)
        self.update_query(query)

    def update_workflow_routing_status(self, status, routine_id):
        query = 'update dss.workflow_routines set status={} where id=\'{}\''.format(status, routine_id)
        print('update_workflow_routing_status|query : ', query)
        self.update_query(query)

    def update_initial_workflow_routing_status(self, status, routine_id):
        query = 'update dss.workflow_routines set status={},last_trigger_date=now()  ' \
                'where id=\'{}\''.format(status, routine_id)
        print('update_initial_workflow_routing_status|query : ', query)
        self.update_query(query)

    def get_next_workflows(self, schedule_date=datetime.now()):
        if type(schedule_date) is datetime:
            schedule_date = schedule_date
        else:
            schedule_date = datetime.strptime(schedule_date, '%Y-%m-%d %H:%M:%S')
        print('schedule_date : ', schedule_date)
        query = 'select id,dss1,dss2,dss3,schedule,cascade_on from dss.workflow_routines where status in (1,3,4,5);'
        print('get_next_workflow_routines|query : ', query)
        results = self.get_multiple_result(query)
        routines = []
        if results is not None:
            for result in results:
                print('get_next_workflow_routines|result : ', result)
                routines.append({'id': result[0], 'dss1': result[1], 'dss2': result[2],
                                 'dss3': result[3], 'schedule': result[4],
                                 'cascade_on': result[5]})
        print('get_next_workflow_routines|routines : ', routines)
        if len(routines) > 0:
            routines = get_next_scheduled_routines(schedule_date, routines)
            if len(routines) > 0:
                for routine in routines:
                    print('update_initial_workflow_routing_status.')
                    self.update_initial_workflow_routing_status(1, routine['id'])
        return routines

    def get_next_workflow_routines(self, schedule_date=datetime.now()):
        if type(schedule_date) is datetime:
            schedule_date = schedule_date
        else:
            schedule_date = datetime.strptime(schedule_date, '%Y-%m-%d %H:%M:%S')
        print('schedule_date : ', schedule_date)
        query = 'select id,dss1,dss2,dss3,schedule,cascade_on from dss.workflow_routines where status in (1,3,4,5);'
        print('get_next_workflow_routines|query : ', query)
        results = self.get_multiple_result(query)
        routines = []
        if results is not None:
            for result in results:
                print('get_next_workflow_routines|result : ', result)
                routines.append({'id': result[0], 'dss1': result[1], 'dss2': result[2],
                                 'dss3': result[3], 'schedule': result[4],
                                 'cascade_on': result[5]})
        print('get_next_workflow_routines|routines : ', routines)
        if len(routines) > 0:
            routine = get_next_scheduled_workflow(schedule_date, routines)
            if routine:
                print('update_initial_workflow_routing_status.')
                self.update_initial_workflow_routing_status(1, routine['id'])
                return routine
            else:
                return None
        else:
            return None

    def get_accuracy_rule_info_by_id(self, rule_id):
        query = 'select id,model_type, model, observed_stations, allowed_error, rule_accuracy ' \
                'from dss.accuracy_rules where id={};'.format(rule_id)
        print('get_accuracy_rule_info_by_id|query: ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        print('get_accuracy_rule_info_by_id|result : ', result)
        if result is not None:
            accuracy_rule = {'id': result[0], 'model_type': result[1], 'model': result[2],
                             'observed_stations': result[3], 'allowed_error': result[4],
                             'rule_accuracy': result[4]}
            return accuracy_rule
        else:
            return None

    # ---------------------------Variable routine-----------------------------
    def update_variable_routing_status(self, status, routine_id):
        query = 'update dss.variable_routines set status={} where id=\'{}\''.format(status, routine_id)
        print('update_variable_routing_status|query : ', query)
        self.update_query(query)

    def update_initial_variable_routing_status(self, status, routine_id):
        query = 'update dss.variable_routines set status={},last_trigger_date=now()  ' \
                'where id=\'{}\''.format(status, routine_id)
        print('update_initial_variable_routing_status|query : ', query)
        self.update_query(query)

    def get_next_variable_routines(self, schedule_date=datetime.now()):
        if type(schedule_date) is datetime:
            schedule_date = schedule_date
        else:
            schedule_date = datetime.strptime(schedule_date, '%Y-%m-%d %H:%M:%S')
        print('schedule_date : ', schedule_date)
        query = 'select id,variable_name,variable_type,dag_name,schedule from dss.variable_routines where status in (1,3,4,5);'
        print('get_next_variable_routines|query : ', query)
        results = self.get_multiple_result(query)
        routines = []
        if results is not None:
            for result in results:
                print('get_next_variable_routines|result : ', result)
                routines.append({'id': result[0], 'variable_name': result[1], 'variable_type': result[2],
                                 'dag_name': result[3], 'schedule': result[4]})
        print('get_next_variable_routines|routines : ', routines)
        if len(routines) > 0:
            routines = get_next_scheduled_routines(schedule_date, routines)
            if len(routines) > 0:
                for routine in routines:
                    print('update_initial_workflow_routing_status.')
                    self.update_initial_variable_routing_status(1, routine['id'])
        return routines

    def update_pump_routing_status(self, status, routine_id):
        query = 'update dss.pump_rules set status={} where id=\'{}\''.format(status, routine_id)
        print('update_pump_routing_status|query : ', query)
        self.update_query(query)

    def update_initial_pump_routing_status(self, status, routine_id):
        query = 'update dss.pump_rules set status={},last_trigger_time=now()  ' \
                'where id=\'{}\''.format(status, routine_id)
        print('update_initial_pump_routing_status|query : ', query)
        self.update_query(query)

    def get_next_pump_routines(self, schedule_date=datetime.now()):
        if type(schedule_date) is datetime:
            schedule_date = schedule_date
        else:
            schedule_date = datetime.strptime(schedule_date, '%Y-%m-%d %H:%M:%S')
        print('schedule_date : ', schedule_date)
        query = 'select id, rule_name, rule_logic, dag_name, status, schedule from dss.pump_rules where status in (1,3,4,5);'
        print('get_next_pump_routines|query : ', query)
        results = self.get_multiple_result(query)
        routines = []
        if results is not None:
            for result in results:
                print('get_next_pump_routines|result : ', result)
                routines.append({'id': result[0], 'rule_name': result[1], 'rule_logic': result[2],
                                 'dag_name': result[3], 'status': result[4], 'schedule': result[5]})
        print('get_next_pump_routines|routines : ', routines)
        if len(routines) > 0:
            routines = get_next_scheduled_routines(schedule_date, routines)
            if len(routines) > 0:
                for routine in routines:
                    print('update_initial_workflow_routing_status.')
                    self.update_initial_pump_routing_status(1, routine['id'])
        return routines

    def evaluate_variable_rule_logic(self, rule_logic):
        sql_query = 'select location from dss.rule_variables where {}'.format(rule_logic)
        print('evaluate_variable_rule_logic|query : ', sql_query)
        results = self.get_multiple_result(sql_query)
        print('evaluate_variable_rule_logic|results : ', results)
        location_names = []
        if results is not None:
            for result in results:
                location_names.append(result[0])
        print('evaluate_variable_rule_logic|location_names : ', location_names)
        return location_names

    # ---------------------------Dynamic dag routine-----------------------------

    def get_dynamic_dag_tasks(self, dag_id):
        sql_query = 'select id, task_name, bash_script, input_params, timeout ' \
                    'from dss.dynamic_tasks where active=1 and dag_id={} order by task_order asc;'.format(dag_id)
        print('get_dynamic_dag_tasks|sql_query : ', sql_query)
        results = self.get_multiple_result(sql_query)
        print('get_dynamic_dag_tasks|results : ', results)
        dag_tasks = []
        if results is not None:
            for result in results:
                if result[3]:
                    dag_tasks.append({'id': result[0], 'task_name': result[1], 'bash_script': result[2],
                                      'input_params': json.loads(result[3]), 'timeout': json.loads(result[4])})
                else:
                    dag_tasks.append({'id': result[0], 'task_name': result[1], 'bash_script': result[2],
                                      'input_params': result[3], 'timeout': json.loads(result[4])})
        print('get_dynamic_dag_tasks|dag_tasks : ', dag_tasks)
        return dag_tasks

    def update_initial_external_bash_routing_status(self, status, routine_id):
        query = 'update dss.dynamic_dags set status={},last_trigger_date=now()  ' \
                'where id=\'{}\''.format(status, routine_id)
        print('update_initial_external_bash_routing_status|query : ', query)
        self.update_query(query)

    def update_external_bash_routing_status(self, status, routine_id):
        query = 'update dss.dynamic_dags set status={} where id=\'{}\''.format(status, routine_id)
        print('update_external_bash_routing_status|query : ', query)
        self.update_query(query)

    def get_external_bash_routines(self, schedule_date=datetime.now()):
        if type(schedule_date) is datetime:
            schedule_date = schedule_date
        else:
            schedule_date = datetime.strptime(schedule_date, '%Y-%m-%d %H:%M:%S')
        print('schedule_date : ', schedule_date)
        query = 'select id, dag_name, schedule, timeout from dss.dynamic_dags where status in (1,3,4,5);'
        print('get_external_bash_routines|query : ', query)
        results = self.get_multiple_result(query)
        routines = []
        if results is not None:
            for result in results:
                print('get_external_bash_routines|result : ', result)
                routines.append({'id': result[0], 'dag_name': result[1], 'schedule': result[2],
                                 'timeout': json.loads(result[3])})
        print('get_external_bash_routines|routines : ', routines)
        if len(routines) > 0:
            routines = get_next_scheduled_routines(schedule_date, routines)
            if len(routines) > 0:
                for routine in routines:
                    print('update_initial_workflow_routing_status.')
                    self.update_initial_external_bash_routing_status(1, routine['id'])
        return routines

    # Dynamic dag generation
    def update_initial_dynamic_dag_routing_status(self, status, routine_id):
        query = 'update dss.dynamic_routine set status={},last_trigger_date=now()  ' \
                'where id=\'{}\''.format(status, routine_id)
        print('update_initial_dynamic_dag_routing_status|query : ', query)
        self.update_query(query)

    def update_dynamic_dag_routing_status(self, status, routine_id):
        query = 'update dss.dynamic_routine set status={} where id=\'{}\''.format(status, routine_id)
        print('update_dynamic_dag_routing_status|query : ', query)
        self.update_query(query)

    def get_dynamic_dag_routines(self, schedule_date=datetime.now()):
        if type(schedule_date) is datetime:
            schedule_date = schedule_date
        else:
            schedule_date = datetime.strptime(schedule_date, '%Y-%m-%d %H:%M:%S')
        print('schedule_date : ', schedule_date)
        query = 'select id, dag_name, schedule, timeout from dss.dynamic_routine where status in (1,3,4,5);'
        print('get_dynamic_dag_routines|query : ', query)
        results = self.get_multiple_result(query)
        routines = []
        if results is not None:
            for result in results:
                print('get_dynamic_dag_routines|result : ', result)
                routines.append({'id': result[0], 'dag_name': result[1], 'schedule': result[2],
                                 'timeout': json.loads(result[3])})
        print('get_dynamic_dag_routines|routines : ', routines)
        if len(routines) > 0:
            routines = get_next_scheduled_routines(schedule_date, routines)
            if len(routines) > 0:
                for routine in routines:
                    print('update_initial_workflow_routing_status.')
                    self.update_initial_dynamic_dag_routing_status(1, routine['id'])
        return routines

    def get_dynamic_dag_routing_status(self, id):
        dag_rule = None
        query = 'select id, status from dss.dynamic_routine where id={}'.format(id)
        print('get_dynamic_dag_routing_status|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            dag_rule = {'id': result[0], 'status': result[1]}
        return dag_rule

    # -----------------------------WRF definition--------------------------------

    def get_namelist_input_configs(self, config_id):
        query = 'select id,run_days, run_hours, run_minutes, run_seconds, interval_seconds, input_from_file, ' \
                'history_interval, frames_per_outfile, restart, restart_interval, io_form_history, io_form_restart, ' \
                'io_form_input, io_form_boundary, debug_level, time_step, time_step_fract_num, time_step_fract_den, ' \
                'max_dom, e_we, e_sn, e_vert, p_top_requested, num_metgrid_levels, num_metgrid_soil_levels, dx, dy, ' \
                'grid_id, parent_id, i_parent_start, j_parent_start, parent_grid_ratio, parent_time_step_ratio, feedback, ' \
                'smooth_option , mp_physics, ra_lw_physics, ra_sw_physics, radt, sf_sfclay_physics, sf_surface_physics,' \
                'bl_pbl_physics, bldt, cu_physics, cudt, isfflx, ifsnow, icloud, surface_input_source, num_soil_layers,' \
                'sf_urban_physics, w_damping, diff_opt, km_opt, diff_6th_opt, diff_6th_factor, base_temp, damp_opt, ' \
                'zdamp, dampcoef, khdif, kvdif, non_hydrostatic, moist_adv_opt, scalar_adv_opt, spec_bdy_width, ' \
                'spec_zone, relax_zone, specified, nested, nio_tasks_per_group, nio_groups ' \
                'from dss.namelist_input_config where id={};'.format(config_id)
        print('get_namelist_input_configs|query: ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        print('get_namelist_input_configs|result : ', result)
        if result is not None:
            wps_config = {'id': result[0], 'run_days': result[1], 'run_hours': result[2], 'run_minutes': result[3],
                          'run_seconds': result[4], 'interval_seconds': result[5],
                          'input_from_file': result[6], 'history_interval': result[7], 'frames_per_outfile': result[8],
                          'restart': result[9], 'restart_interval': result[10], 'io_form_history': result[11],
                          'io_form_restart': result[12], 'io_form_input': result[13], 'io_form_boundary': result[14],
                          'debug_level': result[15], 'time_step': result[16], 'time_step_fract_num': result[17],
                          'time_step_fract_den': result[18], 'max_dom': result[19], 'e_we': result[20],
                          'e_sn': result[21], 'e_vert': result[22], 'p_top_requested': result[23],
                          'num_metgrid_levels': result[24], 'num_metgrid_soil_levels': result[25], 'dx': result[26],
                          'dy': result[27], 'grid_id': result[28], 'parent_id': result[29],
                          'i_parent_start': result[30], 'j_parent_start': result[31], 'parent_grid_ratio': result[32],
                          'parent_time_step_ratio': result[33], 'feedback': result[34], 'smooth_option': result[35],
                          'mp_physics': result[36], 'ra_lw_physics': result[37], 'ra_sw_physics': result[38],
                          'radt': result[39], 'sf_sfclay_physics': result[40], 'sf_surface_physics': result[41],
                          'bl_pbl_physics': result[42], 'bldt': result[43], 'cu_physics': result[44],
                          'cudt': result[45],
                          'isfflx': result[46], 'ifsnow': result[47], 'icloud': result[48],
                          'surface_input_source': result[49], 'num_soil_layers': result[50],
                          'sf_urban_physics': result[51],
                          'w_damping': result[52], 'diff_opt': result[53],
                          'km_opt': result[54], 'diff_6th_opt': result[55], 'diff_6th_factor': result[56],
                          'base_temp': result[57], 'damp_opt': result[58], 'zdamp': result[59],
                          'dampcoef': result[60], 'khdif': result[61], 'kvdif': result[62],
                          'non_hydrostatic': result[63], 'moist_adv_opt': result[64], 'scalar_adv_opt': result[65],
                          'spec_bdy_width': result[66], 'spec_zone': result[67], 'relax_zone': result[68],
                          'specified': result[69], 'nested': result[70], 'nio_tasks_per_group': result[71],
                          'nio_groups': result[72]}
            return wps_config
        else:
            return None

    def set_access_date_namelist_input_configs(self, config_id):
        query = 'update dss.namelist_input_config set last_access_date=now()  ' \
                'where id=\'{}\''.format(config_id)
        print('set_access_date_namelist_input_configs|query : ', query)
        self.update_query(query)

    def get_namelist_wps_configs(self, config_id):
        query = 'select id,wrf_core, max_dom, interval_seconds, io_form_geogrid, parent_id, parent_grid_ratio, ' \
                'i_parent_start, j_parent_start, e_we, e_sn, geog_data_res, dx, ' \
                'dy, map_proj, ref_lat, ref_lon, truelat1, truelat2, ' \
                'stand_lon, geog_data_path, out_format, prefix, fg_name, io_form_metgrid ' \
                'from dss.namelist_wps_config where id={};'.format(config_id)
        print('get_namelist_wps_configs|query: ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        print('get_namelist_wps_configs|result : ', result)
        if result is not None:
            wps_config = {'id': result[0], 'wrf_core': result[1], 'max_dom': result[2],
                          'interval_seconds': result[3], 'io_form_geogrid': result[4],
                          'parent_id': result[5], 'parent_grid_ratio': result[6], 'i_parent_start': result[7],
                          'j_parent_start': result[8], 'e_we': result[9], 'e_sn': result[10],
                          'geog_data_res': result[11], 'dx': result[12], 'dy': result[13],
                          'map_proj': result[14], 'ref_lat': result[15], 'ref_lon': result[16],
                          'truelat1': result[17], 'truelat2': result[18], 'stand_lon': result[19],
                          'geog_data_path': result[20], 'out_format': result[21], 'prefix': result[22],
                          'fg_name': result[23], 'io_form_metgrid': result[24]}
            return wps_config
        else:
            return None

    def set_access_date_namelist_wps_configs(self, config_id):
        query = 'update dss.namelist_wps_config set last_access_date=now()  ' \
                'where id=\'{}\''.format(config_id)
        print('set_access_date_namelist_wps_configs|query : ', query)
        self.update_query(query)

    # -----------------------retrieving rule definition data----------------------------------
    def update_decision_rule_status(self, status, rule_id):
        query = 'update dss.rule_definition set status={},last_access_date=now()  ' \
                'where id=\'{}\''.format(status, rule_id)
        print('update_initial_dynamic_dag_routing_status|query : ', query)
        self.update_query(query)

    def get_all_decision_rules(self):
        query = 'select id,name,logic,success_trigger,fail_trigger,params,timeout from dss.rule_definition ' \
                'where status !=0 ;'
        print('get_all_decision_rules|query : ', query)
        results = self.get_multiple_result(query)
        rules = []
        if results is not None:
            for result in results:
                rule = {'id': result[0], 'name': result[1], 'logic': result[2], 'success_trigger':
                    json.loads(result[3]), 'fail_trigger': json.loads(result[4]),
                        'params': json.loads(result[5]), 'timeout': json.loads(result[6])}
                rules.append(rule)
        return rules

    def get_eligible_decision_rule_definition_by_id(self, rule_id):
        rule_definition = None
        query = 'select id,name,logic,success_trigger,fail_trigger,params,timeout from dss.rule_definition ' \
                'where status in (1, 3, 4, 5) and id={};'.format(rule_id)
        print('get_eligible_decision_rule_definition_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            rule_definition = {'id': result[0], 'name': result[1], 'logic': result[2], 'success_trigger':
                json.loads(result[3]), 'fail_trigger': json.loads(result[4]),
                               'params': json.loads(result[5]), 'timeout': json.loads(result[6])}
        return rule_definition

    def get_decision_rule_definition_by_id(self, rule_id):
        rule_definition = None
        query = 'select id,name,logic,success_trigger,fail_trigger,params,timeout from dss.rule_definition where id={};'.format(
            rule_id)
        print('get_decision_rule_definition_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            rule_definition = {'id': result[0], 'name': result[1], 'logic': result[2], 'success_trigger':
                json.loads(result[3]), 'fail_trigger': json.loads(result[4]),
                               'params': json.loads(result[5]), 'timeout': json.loads(result[6])}
        return rule_definition

    def set_access_date_decision_rule_definitions(self, rule_id):
        query = 'update dss.rule_definition set last_access_date=now()  ' \
                'where id=\'{}\''.format(rule_id)
        print('set_access_date_rule_definitions|query : ', query)
        self.update_query(query)

    def evaluate_decision_rule_logic(self, rule_logic):
        sql_query = 'select exists (select location from dss.rule_variables where {})'.format(rule_logic)
        print('evaluate_rule_logic|query : ', sql_query)
        results = self.get_multiple_result(sql_query)
        print('evaluate_rule_logic|results : ', results)
        if results is not None:
            print('evaluate_rule_logic|results : ', results)
            return results

    def evaluate_rule_logic(self, rule_logic):
        sql_query = 'select location from dss.rule_variables where {}'.format(rule_logic)
        print('evaluate_rule_logic|query : ', sql_query)
        results = self.get_multiple_result(sql_query)
        print('evaluate_rule_logic|results : ', results)
        if results is not None:
            print('evaluate_rule_logic|results : ', results)
            if len(results) > 0:
                return True
            else:
                return False
        else:
            return False

    def get_pump_operating_rules(self, id_list, status=1):
        print('get_pump_operating_rules|id_list : ', id_list)
        if len(id_list) > 0:
            rule_list = []
            for id in id_list:
                sql_query = 'select id,name,logic,flo2d_rule from dss.pump_rules ' \
                            'where status={} and id={};'.format(status, id)
                print('get_pump_operating_rules|sql_query : ', sql_query)
                result = self.get_single_row(sql_query)
                print('get_pump_operating_rules|result : ', result)
                if result is not None:
                    rule_list.append({'id': result[0], 'name': result[1], 'logic': result[2], 'flo2d_rule': result[3]})
            return rule_list
        else:
            print('get_pump_operating_rules|no ids.')
            return []

    def get_logic_rules(self, id_list, status=1):
        print('get_logic_rules|id_list : ', id_list)
        if len(id_list) > 0:
            rule_list = []
            for id in id_list:
                sql_query = 'select id,name,logic from dss.rule_logics ' \
                            'where id={};'.format(status, id)
                print('get_logic_rules|sql_query : ', sql_query)
                result = self.get_single_row(sql_query)
                print('get_logic_rules|result : ', result)
                if result is not None:
                    rule_list.append({'id': result[0], 'name': result[1], 'logic': result[2]})
            return rule_list
        else:
            print('get_logic_rules|no ids.')
            return []

    def set_hechms_rain_tag(self, tag, id):
        print('set_hechms_rain_tag|tag: ', tag)
        query = 'update dss.hechms_rules set rainfall_data_from={} where id=\'{}\''.format(tag, id)
        print('set_hechms_rain_tag|query: ', query)
        self.update_query(query)
        print('set_hechms_rain_tag|tag successfully updated.')

    def set_flo2d_rain_tag(self, tag, id):
        print('set_flo2d_rain_tag|tag: ', tag)
        query = 'update dss.flo2d_rules set raincell_data_from={} where id=\'{}\''.format(tag, id)
        print('set_flo2d_rain_tag|query: ', query)
        self.update_query(query)
        print('set_flo2d_rain_tag|tag successfully updated.')

    def get_parent_dag_tasks(self, dag_rule_id):
        task_param_list = []
        query = 'select input_params from dss.dynamic_workflow where owner_dag_id={}'.format(dag_rule_id)
        print('get_parent_dag_tasks|query: ', query)
        results = self.get_multiple_result(query)
        print('get_parent_dag_tasks|results: ', results)
        for result in results:
            task_param_list.append(json.loads(result[1]))
        return task_param_list



if __name__ == "__main__":
    # db_config = {'mysql_user': 'admin', 'mysql_password': 'floody', 'mysql_host': '35.227.163.211', 'mysql_db': 'dss',
    #              'log_path': '/home/hasitha/PycharmProjects/DSS-Framework/log'}
    db_config = {'mysql_user': 'curw', 'mysql_password': 'cfcwm07', 'mysql_host': '124.43.13.195', 'mysql_db': 'dss',
                 'log_path': '/home/hasitha/PycharmProjects/DSS-Framework/log'}
    adapter = RuleEngineAdapter.get_instance(db_config)
    print(adapter)
    # adapter.get_location_names_from_rule_variables('Precipitation')
    # adapter.get_all_external_bash_routines()
    # adapter.get_external_bash_routines(datetime.now())
    # adapter.get_namelist_input_configs(1)
    adapter.get_rule_definition_by_id(1)
