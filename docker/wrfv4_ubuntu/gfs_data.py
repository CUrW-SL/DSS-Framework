import json
import logging
import multiprocessing
import shutil
from datetime import datetime, timedelta
import math
import time
import os
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from joblib import Parallel, delayed

LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
logging.basicConfig(filename='/home/uwcc-admin/hasitha/Build_WRF/logs/gfs_data.log',
                    level=logging.DEBUG,
                    format=LOG_FORMAT)
log = logging.getLogger()


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


def download_gfs_data(start_date):
    """
    :param start_date: '2017-08-27_00:00'
    :return: 
    """
    log.info('Downloading GFS data: START')
    try:
        with open('wrfv4_config.json') as json_file:
            config = json.load(json_file)
            wrf_conf = get_wrf_config(config['wrf_config'], start_date=start_date)
            
            gfs_date, gfs_cycle, start_inv = get_appropriate_gfs_inventory(wrf_conf)
            inventories = get_gfs_inventory_url_dest_list(gfs_date, wrf_conf.get('period'),
                                                                wrf_conf.get('gfs_url'),
                                                                wrf_conf.get('gfs_inv'), wrf_conf.get('gfs_step'),
                                                                gfs_cycle, wrf_conf.get('gfs_res'),
                                                                wrf_conf.get('gfs_dir'), start=start_inv)
            gfs_threads = wrf_conf.get('gfs_threads')
            log.info(
                'Following data will be downloaded in %d parallel threads\n%s' % (gfs_threads, '\n'.join(
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


if __name__ == '__main__':
    download_gfs_data('2019-08-02_00:00')

