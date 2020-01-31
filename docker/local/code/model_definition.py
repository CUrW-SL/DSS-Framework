import logging
import os

import mysql.connector

LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'

namelist_input_template = '/home/curw/git/DSS-Framework/docker/cloud/namelist/template/template_namelist.input'
namelist_wps_template = '/home/curw/git/DSS-Framework/docker/cloud/namelist/template/template_namelist.wps'


class DSSAdapter:
    __instance = None

    @staticmethod
    def get_instance(db_config):
        """ Static access method. """
        # print('get_instance|db_config : ', db_config)
        if DSSAdapter.__instance is None:
            DSSAdapter(db_config['mysql_user'], db_config['mysql_password'],
                              db_config['mysql_host'], db_config['mysql_db'],
                              db_config['log_path'])
        return DSSAdapter.__instance

    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db, log_path):
        """ Virtually private constructor. """
        if DSSAdapter.__instance is not None:
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
                DSSAdapter.__instance = self
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

    def update_query(self, query):
        cursor = self.cursor
        try:
            print(query)
            cursor.execute(query)
            self.connection.commit()
        except Exception as ex:
            print('update_rule_status|Exception: ', str(ex))
            self.log.error('update_rule_status|query:{}'.format(query))

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
                'spec_zone, relax_zone, specified, nested, nio_tasks_per_group, nio_groups, template_path ' \
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
                          'nio_groups': result[72], 'template_path': result[73]}
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
                'stand_lon, geog_data_path, out_format, prefix, fg_name, io_form_metgrid, template_path ' \
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
                          'fg_name': result[23], 'io_form_metgrid': result[24], 'template_path': result[25]}
            return wps_config
        else:
            return None

    def set_access_date_namelist_wps_configs(self, config_id):
        query = 'update dss.namelist_wps_config set last_access_date=now()  ' \
                'where id=\'{}\''.format(config_id)
        print('set_access_date_namelist_wps_configs|query : ', query)
        self.update_query(query)


def get_db_connection(db_config):
    connection = None
    try:
        connection = mysql.connector.connect(user=db_config['mysql_user'],
                                             password=db_config['mysql_password'],
                                             host=db_config['mysql_host'],
                                             database=db_config['mysql_db'])
    except Exception as e:
        print('get_db_connection|Exception : ', str(e))
    return connection


def get_namelist_wps_config(dss_adapter, config_id, template_path):
    config_data = dss_adapter.get_namelist_wps_configs(config_id)
    content = None
    print('get_namelist_wps_config|config_data : ', config_data)
    if config_data is not None:
        print('get_namelist_wps_configs|config_data : ', config_data)
        with open(template_path, 'r') as file:
            content = file.read()
            content = format_content(content, config_data)
            print('content : ', content)
    return content


def get_namelist_input_config(dss_adapter, config_id, template_path):
    config_data = dss_adapter.get_namelist_input_configs(config_id)
    content = None
    print('get_namelist_input_config|config_data : ', config_data)
    if config_data is not None:
        print('get_namelist_input_config|config_data : ', config_data)
        with open(template_path, 'r') as file:
            content = file.read()
            content = format_content(content, config_data)
            print('content : ', content)
    return content


def format_content(content, config_data):
    for key, value in config_data.items():
        key_str = '{{{}}}'.format(key.upper())
        content = content.replace(key_str, str(value))
    return content


def create_namelist_wps_file(db_config, config_id, file_location):
    try:
        dss_adapter = DSSAdapter.get_instance(db_config)
        config_data = dss_adapter.get_namelist_wps_configs(config_id)
        template_path = config_data['template_path']
        print('get_namelist_wps_config|config_data : ', config_data)
        if config_data is not None:
            print('get_namelist_wps_configs|config_data : ', config_data)
            with open(template_path, 'r') as file:
                content = file.read()
                new_content = format_content(content, config_data)
                print('new_content : ', new_content)
                with open(file_location, 'w') as file:
                    file.write(new_content)
        return True
    except Exception as e:
        print('create_namelist_wps_file|Exception : ', str(e))
        return False


def create_namelist_input_file(db_config, config_id, file_location):
    try:
        dss_adapter = DSSAdapter.get_instance(db_config)
        config_data = dss_adapter.get_namelist_input_configs(config_id)
        template_path = config_data['template_path']
        print('get_namelist_input_config|config_data : ', config_data)
        if config_data is not None:
            print('get_namelist_input_config|config_data : ', config_data)
            with open(template_path, 'r') as file:
                content = file.read()
                new_content = format_content(content, config_data)
                print('new_content : ', new_content)
                with open(file_location, 'w') as file:
                    file.write(new_content)
        return True
    except Exception as e:
        print('create_namelist_input_file|Exception : ', str(e))
        return False


if __name__ == "__main__":
    db_config = {'mysql_user': 'admin', 'mysql_password': 'floody', 'mysql_host': '35.227.163.211', 'mysql_db': 'dss',
                 'log_path': '/home/hasitha/PycharmProjects/DSS-Framework/log'}
    adapter = DSSAdapter.get_instance(db_config)
    print(adapter)
    # get_namelist_wps_config(adapter, 1, namelist_wps_template)
    # get_namelist_input_config(adapter, 1, namelist_input_template)
    namelist_input_file = '/home/hasitha/PycharmProjects/DSS-Framework/output/namelist.input'
    create_namelist_input_file(adapter, 1, namelist_input_template, namelist_input_file)
