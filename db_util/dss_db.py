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
    current_date = datetime.now()
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

    def get_wrf_rule_info(self, status=1):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rules = []
        query = 'select id, name, target_model, version, run, hour, ignore_previous_run, ' \
                'check_gfs_data_availability from dss.wrf_rules ' \
                'where status = {} '.format(status)
        results = self.get_multiple_result(query)
        if results is not None:
            for row in results:
                wrf_rules.append({'id': row[0], 'name': row[1],
                                  'target_model': row[2], 'version': row[3],
                                  'run': row[4], 'hour': row[5],
                                  'ignore_previous_run': row[6],
                                  'check_gfs_data_availability': row[7]})
        return wrf_rules

    def get_wrf_rule_info_by_id(self, rule_id, status=1):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rule = None
        query = 'select id, name, target_model, version, run, hour, ignore_previous_run, ' \
                'check_gfs_data_availability from dss.wrf_rules ' \
                'where id = {} and status = {} '.format(rule_id, status)
        print('get_wrf_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            wrf_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                        'version': result[3], 'run': result[4],
                        'hour': result[5], 'ignore_previous_run': result[6],
                        'check_gfs_data_availability': result[7]}
        return wrf_rule

    def get_eligible_wrf_rule_info_by_id(self, rule_id):
        '''
        :param status:0-disable,1-enable,2-running,3-completed
        :return:[{}{}]
        '''
        wrf_rule = None
        query = 'select id, name, target_model, version, run, hour, ignore_previous_run, ' \
                'check_gfs_data_availability,accuracy_rule, rule_details from dss.wrf_rules ' \
                'where id = {} and status in (1,3,4)  '.format(rule_id)
        print('get_wrf_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            wrf_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                        'version': result[3], 'run': result[4],
                        'hour': result[5], 'ignore_previous_run': result[6],
                        'check_gfs_data_availability': result[7], 'accuracy_rule': result[8],
                        'rule_details': json.loads(result[9])}
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

    def update_wrf_rule_accuracy_level(self, accuracy_level, rule_id):
        query = 'update `dss`.`wrf_rules` set `current_accuracy`=\'{}\' ' \
                'WHERE `id`=\'{}\';'.format(accuracy_level, rule_id)
        print('update_wrf_rule_accuracy_level|query : ', query)
        self.update_query(query)

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

    def get_hechms_rule_info_by_id(self, id, status=1):
        hechms_rule = None
        query = 'select id, name, target_model,forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rainfall_data_from, ' \
                'ignore_previous_run from dss.hechms_rules where status = {} and id = {}'.format(status, id)
        print('get_hechms_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            hechms_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                           'forecast_days': result[3], 'observed_days': result[4],
                           'init_run': result[5], 'no_forecast_continue': result[6],
                           'no_observed_continue': result[7], 'rainfall_data_from': result[8],
                           'ignore_previous_run': result[9]}
        return hechms_rule

    def get_eligible_hechms_rule_info_by_id(self, id):
        hechms_rule = None
        query = 'select id, name, target_model,forecast_days, observed_days, ' \
                'init_run, no_forecast_continue, no_observed_continue, rainfall_data_from, ' \
                'ignore_previous_run, accuracy_rule, rule_details from dss.hechms_rules where ' \
                'status in (1, 3, 4) and id = {}'.format(id)
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

    def get_flo2d_rule_info_by_id(self, id, status=1):
        flo2d_rule = None
        query = 'select id, name, target_model, forecast_days, observed_days, ' \
                'no_forecast_continue, no_observed_continue, raincell_data_from, ' \
                'inflow_data_from, outflow_data_from, ignore_previous_run ' \
                'from dss.flo2d_rules where status={} and id={}'.format(status, id)
        print('get_flo2d_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            flo2d_rule = {'id': result[0], 'name': result[1], 'target_model': result[2],
                          'forecast_days': result[3], 'observed_days': result[4],
                          'no_forecast_continue': result[5], 'no_observed_continue': result[6],
                          'raincell_data_from': result[7], 'inflow_data_from': result[8],
                          'outflow_data_from': result[9], 'ignore_previous_run': result[10]}
        return flo2d_rule

    def get_eligible_flo2d_rule_info_by_id(self, id):
        flo2d_rule = None
        query = 'select id, name, target_model, forecast_days, observed_days, ' \
                'no_forecast_continue, no_observed_continue, raincell_data_from, ' \
                'inflow_data_from, outflow_data_from, ignore_previous_run, accuracy_rule,' \
                'rule_details ' \
                'from dss.flo2d_rules where status in (1, 3, 4) and id={}'.format(id)
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

    def get_flo2d_rule_status_by_id(self, id):
        flo2d_rule = None
        query = 'select id, status from dss.flo2d_rules where id={}'.format(id)
        print('get_flo2d_rule_info_by_id|query : ', query)
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is not None:
            flo2d_rule = {'id': result[0], 'status': result[1]}
        return flo2d_rule

    def update_flo2d_rule_accuracy_level(self, accuracy_level, rule_id):
        query = 'update `dss`.`flo2d_rules` set `current_accuracy`=\'{}\' ' \
                'WHERE `id`=\'{}\';'.format(accuracy_level, rule_id)
        print('update_flo2d_rule_accuracy_level|query : ', query)
        self.update_query(query)

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
        query = 'select id,dss1,dss2,dss3,schedule,cascade_on from dss.workflow_routines where status in (1,3);'
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
        query = 'select id,dss1,dss2,dss3,schedule,cascade_on from dss.workflow_routines where status in (1,3);'
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
        query = 'select id,variable_name,variable_type,dag_name,schedule from dss.variable_routines where status in (1,3);'
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
        query = 'update dss.pump_rules set status={},last_trigger_date=now()  ' \
                'where id=\'{}\''.format(status, routine_id)
        print('update_initial_pump_routing_status|query : ', query)
        self.update_query(query)

    def get_next_pump_routines(self, schedule_date=datetime.now()):
        if type(schedule_date) is datetime:
            schedule_date = schedule_date
        else:
            schedule_date = datetime.strptime(schedule_date, '%Y-%m-%d %H:%M:%S')
        print('schedule_date : ', schedule_date)
        query = 'select id, rule_name, rule_logic, dag_name, status, schedule from dss.pump_rules where status in (1,3);'
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

    def update_initial_external_bash_routing_status(self, status, routine_id):
        query = 'update dss.dynamic_dags set status={},last_trigger_date=now()  ' \
                'where id=\'{}\''.format(status, routine_id)
        print('update_initial_external_bash_routing_status|query : ', query)
        self.update_query(query)

    def get_external_bash_routines(self, schedule_date=datetime.now()):
        if type(schedule_date) is datetime:
            schedule_date = schedule_date
        else:
            schedule_date = datetime.strptime(schedule_date, '%Y-%m-%d %H:%M:%S')
        print('schedule_date : ', schedule_date)
        query = 'select id, dag_name, schedule, timeout from dss.dynamic_dags where status in (1,3);'
        print('get_external_bash_routines|query : ', query)
        results = self.get_multiple_result(query)
        routines = []
        if results is not None:
            for result in results:
                print('get_external_bash_routines|result : ', result)
                routines.append({'id': result[0], 'dag_name': result[1], 'schedule': result[2],
                                 'timeout': result[3]})
        print('get_external_bash_routines|routines : ', routines)
        if len(routines) > 0:
            routines = get_next_scheduled_routines(schedule_date, routines)
            if len(routines) > 0:
                for routine in routines:
                    print('update_initial_workflow_routing_status.')
                    self.update_initial_external_bash_routing_status(1, routine['id'])
        return routines

    def get_location_names_from_rule_variables(self, variable_type):
        print('get_location_names_from_rule_variables|variable_type : ', variable_type)
        query = 'select location_name from dss.rule_variables where variable_type=\'{}\';'.format(variable_type)
        print('get_next_variable_routines|query : ', query)
        results = self.get_multiple_result(query)
        locations = []
        if results is not None:
            for result in results:
                locations.append(result[0])
        print('locations : ', locations)
        return locations

    def update_variable_value(self, variable_name, variable_value, variable_type, location_name):
        if variable_type == 'Precipitation':
            sql_query = 'update `dss`.`rule_variables` set `{}`=\'{}\' ' \
                        'where `variable_type`=\'{}\' and`location_name`=\'{}\';'.format(variable_name, variable_value,
                                                                                         variable_type, location_name)
        elif variable_type == 'WaterLevel':
            sql_query = 'update `dss`.`rule_variables` set `{}`=\'{}\' ' \
                        'where `variable_type`=\'{}\' and`location_name`=\'{}\';'.format(variable_name, variable_value,
                                                                                         variable_type, location_name)
        else:
            print('Invalid_variable type|variable_type : ', variable_type)
        print('update_variable_value|query : ', sql_query)
        self.update_query(sql_query)

    def evaluate_variable_rule_logic(self, rule_logic):
        sql_query = 'select location_name from dss.rule_variables where {}'.format(rule_logic)
        print('evaluate_variable_rule_logic|query : ', sql_query)
        results = self.get_multiple_result(sql_query)
        print('evaluate_variable_rule_logic|results : ', results)
        location_names = []
        if results is not None:
            for result in results:
                location_names.append(result[0])
        print('evaluate_variable_rule_logic|location_names : ', location_names)
        return location_names


if __name__ == "__main__":
    db_config = {'mysql_user': 'admin', 'mysql_password': 'floody', 'mysql_host': '35.227.163.211', 'mysql_db': 'dss',
                 'log_path': '/home/hasitha/PycharmProjects/DSS-Framework/log'}
    adapter = RuleEngineAdapter.get_instance(db_config)
    print(adapter)
    adapter.get_location_names_from_rule_variables('Precipitation')
