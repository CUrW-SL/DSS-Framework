import glob
import json
import logging
import multiprocessing
import ntpath
import re
import shlex
import shutil
import subprocess
from datetime import datetime, timedelta
import math
import time
import os
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from zipfile import ZipFile, ZIP_DEFLATED

import pkg_resources
from joblib import Parallel, delayed
#from docker.wrfv4_ubuntu import constants
import constants


LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
logging.basicConfig(filename='/home/uwcc-admin/hasitha/Build_WRF/logs/wrf_preprocessing.log',
                    level=logging.DEBUG,
                    format=LOG_FORMAT)
log = logging.getLogger()


class GfsDataUnavailable(Exception):
    def __init__(self, msg, missing_data):
        self.msg = msg
        self.missing_data = missing_data
        Exception.__init__(self, 'Unable to download %s' % msg)


class UnableFindResource(Exception):
    def __init__(self, res):
        Exception.__init__(self, 'Unable to find %s' % res)


def get_resource_path(resource):
    res = pkg_resources.resource_filename(__name__, resource)
    if os.path.exists(res):
        return res
    else:
        raise UnableFindResource(resource)


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_wrf_config(wrf_config, start_date=None, **kwargs):
    """
    precedence = kwargs > wrf_config.json > constants
    """
    if start_date is not None:
        wrf_config['start_date'] = start_date

    for key in kwargs:
        wrf_config[key] = kwargs[key]

    return wrf_config


def file_exists_nonempty(filename):
    return os.path.exists(filename) and os.path.isfile(filename) and os.stat(filename).st_size != 0


def download_file(url, dest, retries=0, delay=60, overwrite=False, secondary_dest_dir=None):
    try_count = 1
    last_e = None

    def _download_file(_url, _dest):
        _f = urlopen(_url)
        with open(_dest, "wb") as _local_file:
            _local_file.write(_f.read())

    while try_count <= retries + 1:
        try:
            log.info("Downloading %s to %s" % (url, dest))
            if secondary_dest_dir is None:
                if not overwrite and file_exists_nonempty(dest):
                    log.info('File already exists. Skipping download!')
                else:
                    _download_file(url, dest)
                return
            else:
                secondary_file = os.path.join(secondary_dest_dir, os.path.basename(dest))
                if file_exists_nonempty(secondary_file):
                    log.info("File available in secondary dir. Copying to the destination dir from secondary dir")
                    shutil.copyfile(secondary_file, dest)
                else:
                    log.info("File not available in secondary dir. Downloading...")
                    _download_file(url, dest)
                    log.info("Copying to the secondary dir")
                    shutil.copyfile(dest, secondary_file)
                return

        except (HTTPError, URLError) as e:
            log.error(
                'Error in downloading %s Attempt %d : %s . Retrying in %d seconds' % (url, try_count, e.message, delay))
            try_count += 1
            last_e = e
            time.sleep(delay)
        except FileExistsError:
            log.info('File was already downloaded by another process! Returning')
            return
    raise last_e


def get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, fcst_id, res, gfs_dir):
    url0 = url.replace('YYYY', date_str[0:4]).replace('MM', date_str[4:6]).replace('DD', date_str[6:8]).replace('CC',
                                                                                                                cycle)
    inv0 = inv.replace('CC', cycle).replace('FFF', fcst_id).replace('RRRR', res).replace('YYYY', date_str[0:4]).replace(
        'MM', date_str[4:6]).replace('DD', date_str[6:8])

    dest = os.path.join(gfs_dir, date_str + '.' + inv0)
    return url0 + inv0, dest


def get_gfs_inventory_url_dest_list(date, period, url, inv, step, cycle, res, gfs_dir, start=0):
    date_str = date.strftime('%Y%m%d') if type(date) is datetime else date
    return [get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, str(i).zfill(3), res, gfs_dir) for i in
            range(start, start + int(period * 24) + 1, step)]


def datetime_to_epoch(timestamp=None):
    timestamp = datetime.now() if timestamp is None else timestamp
    return (timestamp - datetime(1970, 1, 1)).total_seconds()


def epoch_to_datetime(epoch_time):
    return datetime(1970, 1, 1) + timedelta(seconds=epoch_time)


def datetime_floor(timestamp, floor_sec):
    return epoch_to_datetime(math.floor(datetime_to_epoch(timestamp) / floor_sec) * floor_sec)


def get_appropriate_gfs_inventory(wrf_config):
    st = datetime_floor(datetime.strptime(wrf_config.get('start_date'), '%Y-%m-%d_%H:%M'), 3600 * wrf_config.get(
        'gfs_step'))
    # if the time difference between now and start time is lt gfs_lag, then the time will be adjusted
    if (datetime.utcnow() - st).total_seconds() <= wrf_config.get('gfs_lag') * 3600:
        floor_val = datetime_floor(st - timedelta(hours=wrf_config.get('gfs_lag')), 6 * 3600)
    else:
        floor_val = datetime_floor(st, 6 * 3600)
    gfs_date = floor_val.strftime('%Y%m%d')
    gfs_cycle = str(floor_val.hour).zfill(2)
    start_inv = math.floor((st - floor_val).total_seconds() / 3600 / wrf_config.get('gfs_step')) * wrf_config.get(
        'gfs_step')

    return gfs_date, gfs_cycle, start_inv


def download_parallel(url_dest_list, procs=multiprocessing.cpu_count(), retries=0, delay=60, overwrite=False,
                      secondary_dest_dir=None):
    Parallel(n_jobs=procs)(
        delayed(download_file)(i[0], i[1], retries, delay, overwrite, secondary_dest_dir) for i in url_dest_list)


def download_gfs_data(start_date, wrf_conf):
    """
    :param start_date: '2017-08-27_00:00'
    :return:
    """
    log.info('Downloading GFS data: START')
    try:
        gfs_date, gfs_cycle, start_inv = get_appropriate_gfs_inventory(wrf_conf)
        inventories = get_gfs_inventory_url_dest_list(gfs_date, wrf_conf.get('period'),
                                                          wrf_conf.get('gfs_url'),
                                                          wrf_conf.get('gfs_inv'), wrf_conf.get('gfs_step'),
                                                          gfs_cycle, wrf_conf.get('gfs_res'),
                                                          wrf_conf.get('gfs_dir'), start=start_inv)
        gfs_threads = wrf_conf.get('gfs_threads')
        log.info('Following data will be downloaded in %d parallel threads\n%s' % (gfs_threads, '\n'.join(
                    ' '.join(map(str, i)) for i in inventories)))

        start_time = time.time()
        download_parallel(inventories, procs=gfs_threads, retries=wrf_conf.get('gfs_retries'),
                              delay=wrf_conf.get('gfs_delay'), secondary_dest_dir=None)

        elapsed_time = time.time() - start_time
        log.info('Downloading GFS data: END Elapsed time: %f' % elapsed_time)
        log.info('Downloading GFS data: END')
        return gfs_date, start_inv
    except Exception as e:
        log.error('Downloading GFS data error: {}'.format(str(e)))


def get_gfs_data_dest(inv, date_str, cycle, fcst_id, res, gfs_dir):
    inv0 = inv.replace('CC', cycle).replace('FFF', fcst_id).replace('RRRR', res)
    dest = os.path.join(gfs_dir, date_str + '.' + inv0)
    return dest


def get_gfs_inventory_dest_list(date, period, inv, step, cycle, res, gfs_dir):
    date_str = date.strftime('%Y%m%d')
    return [get_gfs_data_dest(inv, date_str, cycle, str(i).zfill(3), res, gfs_dir) for i in
            range(0, period * 24 + 1, step)]


def check_gfs_data_availability(date, wrf_config):
    log.info('Checking gfs data availability...')
    inventories = get_gfs_inventory_dest_list(date, wrf_config['period'], wrf_config['gfs_inv'],
                                              wrf_config['gfs_step'], wrf_config['gfs_cycle'],
                                              wrf_config['gfs_res'], wrf_config['gfs_dir'])
    missing_inv = []
    for inv in inventories:
        if not os.path.exists(inv):
            missing_inv.append(inv)

    if len(missing_inv) > 0:
        log.error('Some data unavailable')
        raise GfsDataUnavailable('Some data unavailable', missing_inv)

    log.info('GFS data available')


def get_wps_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, constants.DEFAULT_WPS_PATH)


def replace_file_with_values(source, destination, val_dict):
    log.debug('replace file source ' + source)
    log.debug('replace file destination ' + destination)
    log.debug('replace file content dict ' + str(val_dict))
    # pattern = re.compile(r'\b(' + '|'.join(val_dict.keys()) + r')\b')
    pattern = re.compile('|'.join(list(val_dict.keys())))

    with open(destination, 'w') as dest:
        out = ''
        with open(source, 'r') as src:
            line = pattern.sub(lambda x: val_dict[x.group()], src.read())
            dest.write(line)
            out += line

    log.debug('replace file final content \n' + out)


def replace_file_with_values_with_dates(wrf_config, src, dest, aux_dict, start_date=None, end_date=None):
    if start_date is None:
        start_date = datetime_floor(datetime.strptime(wrf_config['start_date'], '%Y-%m-%d_%H:%M'),
                                          wrf_config['gfs_step'] * 3600)

    if end_date is None:
        end_date = start_date + timedelta(days=wrf_config['period'])

    period = wrf_config['period']

    d = {
        'YYYY1': start_date.strftime('%Y'),
        'MM1': start_date.strftime('%m'),
        'DD1': start_date.strftime('%d'),
        'hh1': start_date.strftime('%H'),
        'mm1': start_date.strftime('%M'),
        'YYYY2': end_date.strftime('%Y'),
        'MM2': end_date.strftime('%m'),
        'DD2': end_date.strftime('%d'),
        'hh2': end_date.strftime('%H'),
        'mm2': end_date.strftime('%M'),
        'GEOG': wrf_config['geog_dir'],
        'RD0': str(int(period)),
        'RH0': str(int(period * 24 % 24)),
        'RM0': str(int(period * 60 * 24 % 60)),
        'hi1': '180',
        'hi2': '60',
        'hi3': '60',
    }

    if aux_dict and aux_dict in wrf_config:
        d.update(wrf_config[aux_dict])

    replace_file_with_values(src, dest, d)


def replace_namelist_wps(wrf_config, start_date=None, end_date=None):
    log.info('Replacing namelist.wps...')
    if os.path.exists(wrf_config['namelist_wps']):
        f = wrf_config['namelist_wps']
    else:
        f = get_resource_path(os.path.join('execution', constants.DEFAULT_NAMELIST_WPS_TEMPLATE))

    dest = os.path.join(get_wps_dir(wrf_config['wrf_home']), 'namelist.wps')
    start_date = datetime.strptime(start_date, '%Y-%m-%d_%H:%M')
    replace_file_with_values_with_dates(wrf_config, f, dest, 'namelist_wps_dict', start_date, end_date)


def delete_files_with_prefix(src_dir, prefix):
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        os.remove(filename)


def get_appropriate_gfs_inventory(wrf_config):
    st = datetime_floor(datetime.strptime(wrf_config['start_date'], '%Y-%m-%d_%H:%M'), 3600 * wrf_config['gfs_step'])
    # if the time difference between now and start time is lt gfs_lag, then the time will be adjusted
    if (datetime.utcnow() - st).total_seconds() <= wrf_config['gfs_lag'] * 3600:
        floor_val = datetime_floor(st - timedelta(hours=wrf_config['gfs_lag']), 6 * 3600)
    else:
        floor_val = datetime_floor(st, 6 * 3600)
    gfs_date = floor_val.strftime('%Y%m%d')
    gfs_cycle = str(floor_val.hour).zfill(2)
    start_inv = math.floor((st - floor_val).total_seconds() / 3600 / wrf_config['gfs_step']) * wrf_config['gfs_step']
    return gfs_date, gfs_cycle, start_inv


def run_subprocess(cmd, cwd=None, print_stdout=False):
    log.info('Running subprocess %s cwd %s' % (cmd, cwd))
    start_t = time.time()
    output = ''
    try:
        output = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, cwd=cwd)
    except subprocess.CalledProcessError as e:
        log.error('Exception in subprocess %s! Error code %d' % (cmd, e.returncode))
        log.error(e.output)
        raise e
    finally:
        elapsed_t = time.time() - start_t
        log.info('Subprocess %s finished in %f s' % (cmd, elapsed_t))
        if print_stdout:
            log.info('stdout and stderr of %s\n%s' % (cmd, output))
    return output


def move_files_with_prefix(src_dir, prefix, dest_dir):
    create_dir_if_not_exists(dest_dir)
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        shutil.move(filename, os.path.join(dest_dir, ntpath.basename(filename)))


def check_geogrid_output(wps_dir):
    for i in range(1, 4):
        if not os.path.exists(os.path.join(wps_dir, 'geo_em.d%02d.nc' % i)):
            return False
    return True


def move_files_with_prefix(src_dir, prefix, dest_dir):
    create_dir_if_not_exists(dest_dir)
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        shutil.move(filename, os.path.join(dest_dir, ntpath.basename(filename)))


def create_zip_with_prefix(src_dir, regex, dest_zip, comp=ZIP_DEFLATED, clean_up=False):
    with ZipFile(dest_zip, 'w', compression=comp) as zip_file:
        for filename in glob.glob(os.path.join(src_dir, regex)):
            zip_file.write(filename, arcname=os.path.basename(filename))
            if clean_up:
                os.remove(filename)
    return dest_zip


def run_wps(wrf_config):
    log.info('Running WPS: START')
    wrf_home = wrf_config['wrf_home']
    wps_dir = get_wps_dir(wrf_home)
    output_dir = create_dir_if_not_exists(
        os.path.join(wrf_config['nfs_dir'], 'results', wrf_config['run_id'], 'wps'))

    log.info('Cleaning up files')
    logs_dir = create_dir_if_not_exists(os.path.join(output_dir, 'logs'))

    delete_files_with_prefix(wps_dir, 'FILE:*')
    delete_files_with_prefix(wps_dir, 'PFILE:*')
    delete_files_with_prefix(wps_dir, 'met_em*')

    # Linking VTable
    if not os.path.exists(os.path.join(wps_dir, 'Vtable')):
        log.info('Creating Vtable symlink')
        os.symlink(os.path.join(wps_dir, 'ungrib/Variable_Tables/Vtable.NAM'), os.path.join(wps_dir, 'Vtable'))

    # Running link_grib.csh
    gfs_date, gfs_cycle, start = get_appropriate_gfs_inventory(wrf_config)
    dest = get_gfs_data_url_dest_tuple(wrf_config['gfs_url'], wrf_config['gfs_inv'], gfs_date, gfs_cycle,
                                             '', wrf_config['gfs_res'], '')[1].replace('.grb2', '')
    run_subprocess(
        'csh link_grib.csh %s/%s' % (wrf_config['gfs_dir'], dest), cwd=wps_dir)
    try:
        # Starting ungrib.exe
        try:
            run_subprocess('./ungrib.exe', cwd=wps_dir)
        finally:
            move_files_with_prefix(wps_dir, 'ungrib.log', logs_dir)
        # Starting geogrid.exe'
        if not check_geogrid_output(wps_dir):
            logging.info('Geogrid output not available')
            try:
                run_subprocess('./geogrid.exe', cwd=wps_dir)
            finally:
                move_files_with_prefix(wps_dir, 'geogrid.log', logs_dir)
        # Starting metgrid.exe'
        try:
            run_subprocess('./metgrid.exe', cwd=wps_dir)
        finally:
            move_files_with_prefix(wps_dir, 'metgrid.log', logs_dir)
    finally:
        log.info('Moving namelist wps file')
        move_files_with_prefix(wps_dir, 'namelist.wps', output_dir)

    log.info('Running WPS: DONE')

    log.info('Zipping metgrid data')
    metgrid_zip = os.path.join(wps_dir, wrf_config['run_id'] + '_metgrid.zip')
    create_zip_with_prefix(wps_dir, 'met_em.d*', metgrid_zip)

    log.info('Moving metgrid data')
    dest_dir = os.path.join(wrf_config['nfs_dir'], 'metgrid')
    move_files_with_prefix(wps_dir, metgrid_zip, dest_dir)


if __name__ == '__main__':
    with open('wrfv4_config.json') as json_file:
        config = json.load(json_file)
        wrf_conf = config['wrf_config']
        #download_gfs_data('2019-08-02_00:00', wrf_conf)
        #replace_namelist_wps(wrf_conf, '2019-08-02_00:00')
        wrf_conf['run_id'] = 'test_run1_04_02_2019'
        wrf_conf['start_date'] = '2019-08-02_00:00'
        #run_wps(wrf_conf)
        log.info('Cleaning up wps dir...')
        wps_dir = get_wps_dir(wrf_conf['wrf_home'])
        shutil.rmtree(wrf_conf['gfs_dir'])
        delete_files_with_prefix(wps_dir, 'FILE:*')
        delete_files_with_prefix(wps_dir, 'PFILE:*')
        delete_files_with_prefix(wps_dir, 'geo_em.*')

