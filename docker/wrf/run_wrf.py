import argparse
import json
import logging
import os
import shutil

from docker.wrf.wrf_run import utils
from docker.wrf.wrf_run import docker_utils
from docker.wrf.wrf_run import executor


def parse_args():
    parser = argparse.ArgumentParser()
    env_vars = docker_utils.get_env_vars('CURW_')

    def check_key(k, d_val):
        if k in env_vars and not env_vars[k]:
            return env_vars[k]
        else:
            return d_val

    parser.add_argument('-run_id', default=check_key('run_id', docker_utils.id_generator()))
    parser.add_argument('-mode', default=check_key('mode', 'wps'))
    parser.add_argument('-nl_wps', default=check_key('nl_wps', None))
    parser.add_argument('-nl_input', default=check_key('nl_input', None))
    parser.add_argument('-wrf_config', default=check_key('wrf_config', '{}'))

    return parser.parse_args()


def run_wrf(wrf_config):
    logging.info('Running WRF')

    logging.info('Replacing the namelist input file')
    executor.replace_namelist_input(wrf_config)

    logging.info('Running WRF...')
    executor.run_em_real(wrf_config)


def run_wps(wrf_config):
    logging.info('Downloading GFS data')
    executor.download_gfs_data(wrf_config)

    logging.info('Replacing the namelist wps file')
    executor.replace_namelist_wps(wrf_config)

    logging.info('Running WPS...')
    executor.run_wps(wrf_config)

    logging.info('Cleaning up wps dir...')
    wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))
    shutil.rmtree(wrf_config.get('gfs_dir'))
    utils.delete_files_with_prefix(wps_dir, 'FILE:*')
    utils.delete_files_with_prefix(wps_dir, 'PFILE:*')
    utils.delete_files_with_prefix(wps_dir, 'geo_em.*')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    try:
        args = vars(parse_args())
        logging.info('Running arguments:\n%s' % json.dumps(args, sort_keys=True, indent=0))
        wrf_model = args['wrf_model']
        logging.info('**** WRF RUN **** wrf_model: ' + wrf_model)
        gfs_hour = args['gfs_hour']
        logging.info('**** WRF RUN **** gfs_hour: ' + gfs_hour)
        wrf_config = args['wrf_config']
        logging.info('**** WRF RUN **** wrf_config: ' + wrf_config)
        with open('config_rainfall.json') as json_file:
            config = json.load(json_file)
            logging.info('WRF config: %s' % config.to_json_string())
            namelist_key = 'namelist_{}'.format(wrf_model)
            if namelist_key in config:
                namelist_file = config[namelist_key]
    except Exception as e:
        print('config reading exception:', str(e))

    if mode == 'wps':
        logging.info('Running WPS')
        write_wps()
        run_wps(config)
    elif mode == 'wrf':
        logging.info('Running WRF')
        write_input()
        run_wrf(config)
    elif mode == "all":
        logging.info("Running both WPS and WRF")
        write_wps()
        write_input()
        run_wps(config)
        run_wrf(config)
    elif mode == "test":
        logging.info("Running on test mode: Nothing to do!")
        logging.info('namelist.wps content: \n%s' % docker_utils.get_base64_decoded_str(nl_wps))
        logging.info('namelist.input content: \n%s' % docker_utils.get_base64_decoded_str(nl_input))
    else:
        raise docker_utils.CurwDockerRainfallException('Unknown mode ' + mode)

