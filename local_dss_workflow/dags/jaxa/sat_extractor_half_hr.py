import argparse
import ast
import datetime as dt
import glob
import logging
import multiprocessing
import ntpath
import os
import shutil
import tempfile
import time
import zipfile
from random import random
import sys
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import matplotlib
import numpy as np
import imageio
from mpl_toolkits.basemap import Basemap
import math

from joblib import Parallel, delayed
from mpl_toolkits.basemap import cm

from curwmysqladapter import Station

matplotlib.use('Agg')
from matplotlib import colors, pyplot as plt

DEFAULT_WRF_HOME = '/mnt/disks/wrf-mod/'


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def copy_files_with_prefix(src_dir, prefix, dest_dir):
    create_dir_if_not_exists(dest_dir)
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        shutil.copy(filename, os.path.join(dest_dir, ntpath.basename(filename)))


def extract_jaxa_satellite_hourly_data(ts, output_dir):
    ts = datetime_floor(ts, 3600)
    logging.info('Jaxa satellite data extraction for %s (previous hour)' % str(ts))
    extract_jaxa_satellite_data(ts - dt.timedelta(hours=1), ts, output_dir)


def create_daily_gif(start, output_dir, output_filename, output_prefix):
    tmp_dir = tempfile.mkdtemp(prefix='tmp_jaxa_daily')

    copy_files_with_prefix(output_dir, output_prefix + '_' + start.strftime('%Y-%m-%d') + '*.png', tmp_dir)
    logging.info('Writing gif ' + output_filename)
    gif_list = [os.path.join(tmp_dir, i) for i in sorted(os.listdir(tmp_dir))]
    if len(gif_list) > 0:
        create_gif(gif_list, os.path.join(output_dir, output_filename))
    else:
        logging.info('No images found to create the gif')

    logging.info('Cleaning up ' + tmp_dir)
    shutil.rmtree(tmp_dir)


def create_gif(filenames, output, duration=0.5):
    images = []
    for filename in filenames:
        images.append(imageio.imread(filename))
    imageio.mimsave(output, images, duration=duration)


def delete_files_with_prefix(src_dir, prefix):
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        os.remove(filename)


def extract_jaxa_satellite_data_every_half_hr(exe_ts_utc, output_dir, cleanup=True, cum=False, tmp_dir=None,
                                              lat_min=5.722969, lon_min=79.52146, lat_max=10.06425, lon_max=82.18992,
                                              output_prefix='jaxa_sat', db_adapter_config=None):
    exe_ts_utc = exe_ts_utc - dt.timedelta(minutes=2)
    print('-------------extract_jaxa_satellite_data_half_hr---------------exe_ts_utc:', exe_ts_utc)
    exe_ts_utc = exe_ts_utc - dt.timedelta(minutes=30)
    run_minute = int(exe_ts_utc.strftime('%M'))
    print('run_minute : ', run_minute)
    year_str = exe_ts_utc.strftime('%Y')
    month_str = exe_ts_utc.strftime('%m')
    day_str = exe_ts_utc.strftime('%d')
    hour_str = exe_ts_utc.strftime('%H')
    hour_str1 = (exe_ts_utc + dt.timedelta(hours=1)).strftime('%H')
    # if run_minute == 0:
    #    url = 'ftp://rainmap:Niskur+1404@hokusai.eorc.jaxa.jp/now/txt/05_AsiaSS/gsmap_now.{}{}{}.{}00_{}59.05_AsiaSS.csv.zip' \
    #        .format(year_str, month_str, day_str, hour_str, hour_str)
    # else:
    #    url = 'ftp://rainmap:Niskur+1404@hokusai.eorc.jaxa.jp/now/txt/05_AsiaSS/gsmap_now.{}{}{}.{}30_{}29.05_AsiaSS.csv.zip' \
    #        .format(year_str, month_str, day_str, hour_str, hour_str1)

    if run_minute == 0:
        url = 'ftp://rainmap:Niskur+1404@hokusai.eorc.jaxa.jp/now/txt/island/SriLanka/gsmap_now.{}{}{}.{}00_{}59.SriLanka.csv.zip' \
            .format(year_str, month_str, day_str, hour_str, hour_str)
    else:
        url = 'ftp://rainmap:Niskur+1404@hokusai.eorc.jaxa.jp/now/txt/island/SriLanka/gsmap_now.{}{}{}.{}30_{}29.SriLanka.csv.zip' \
            .format(year_str, month_str, day_str, hour_str, hour_str1)

    print('Download url : ', url)
    start_time = exe_ts_utc
    end_time = start_time + dt.timedelta(hours=1)
    print('_get_start_end|[start_time, end_time] : ', [start_time, end_time])

    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp(prefix='tmp_jaxa')

    print('Download url : ', url)
    url_dest_list = [(url, os.path.join(tmp_dir, os.path.basename(url)),
                      os.path.join(output_dir,
                                   output_prefix + '_' + exe_ts_utc.strftime('%Y-%m-%d_%H:%M') + '.asc'))]

    procs = multiprocessing.cpu_count()

    logging.info('Downloading inventory in parallel')
    download_parallel(url_dest_list, procs)
    logging.info('Downloading inventory complete')

    logging.info('Processing files in parallel')
    Parallel(n_jobs=procs)(
        delayed(process_jaxa_zip_file)(i[1], i[2], lat_min, lon_min, lat_max, lon_max, cum, output_prefix,
                                       db_adapter_config) for i in url_dest_list)
    logging.info('Processing files complete')

    logging.info('Creating sat rf gif for today')
    create_daily_gif(exe_ts_utc, output_dir, output_prefix + '_today.gif', output_prefix)

    prev_day_gif = os.path.join(output_dir, output_prefix + '_yesterday.gif')
    if not file_exists_nonempty(prev_day_gif) or exe_ts_utc.strftime('%H:%M') == '00:00':
        logging.info('Creating sat rf gif for yesterday')
        create_daily_gif(datetime_floor(exe_ts_utc, 3600 * 24) - dt.timedelta(days=1), output_dir,
                         output_prefix + '_yesterday.gif', output_prefix)

    if cum:
        logging.info('Processing cumulative')
        process_cumulative_plot(url_dest_list, start_time, end_time, output_dir, lat_min, lon_min, lat_max, lon_max)
        logging.info('Processing cumulative complete')

    # clean up temp dir
    if cleanup:
        logging.info('Cleaning up')
        shutil.rmtree(tmp_dir)
        delete_files_with_prefix(output_dir, '*.archive')


def extract_jaxa_satellite_data_half_hr(exe_ts_utc, output_dir, cleanup=True, cum=False, tmp_dir=None,
                                        lat_min=5.722969, lon_min=79.52146, lat_max=10.06425, lon_max=82.18992,
                                        output_prefix='jaxa_sat', db_adapter_config=None):
    print('-------------extract_jaxa_satellite_data_half_hr---------------exe_ts_utc:', exe_ts_utc)
    exe_ts_utc = exe_ts_utc - dt.timedelta(hours=1)
    login = 'rainmap:Niskur+1404'
    url_hour = 'ftp://' + login + '@hokusai.eorc.jaxa.jp/now/txt/05_AsiaSS/gsmap_now.YYYYMMDD.HH00_HH59.05_AsiaSS.csv.zip'
    url_half_hour = 'ftp://' + login + '@hokusai.eorc.jaxa.jp/now/txt/05_AsiaSS/gsmap_now.YYYYMMDD.HH30_KK29.05_AsiaSS.csv.zip'

    run_minute = int(exe_ts_utc.strftime('%M'))
    print('run_minute : ', run_minute)

    remainder = run_minute % 30
    run_minute = run_minute - remainder
    start_time = exe_ts_utc - dt.timedelta(minutes=remainder)
    end_time = start_time + dt.timedelta(minutes=30)
    print('_get_start_end|[start_time, end_time] : ', [start_time, end_time])
    if run_minute == 0:
        exe_ts_utc = exe_ts_utc.strftime('%Y-%m-%d %H:00:00')

    else:
        exe_ts_utc = exe_ts_utc.strftime('%Y-%m-%d %H:30:00')

    exe_ts_utc = dt.datetime.strptime(exe_ts_utc, '%Y-%m-%d %H:%M:%S')

    def _get_download_url(run_minute):
        remainder = run_minute % 30
        run_minute = run_minute - remainder
        if run_minute == 0:
            return url_hour
        else:
            return url_half_hour

    def _format_url(url):
        ph = {'YYYY': exe_ts_utc.strftime('%Y'),
              'MM': exe_ts_utc.strftime('%m'),
              'DD': exe_ts_utc.strftime('%d'),
              'KK': (exe_ts_utc + dt.timedelta(hours=1)).strftime('%H'),
              'HH': exe_ts_utc.strftime('%H')}
        for k, v in list(ph.items()):
            url = url.replace(k, v)
        print('url : ', url)
        return url

    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp(prefix='tmp_jaxa')

    url = _get_download_url(run_minute)
    formatted_url = _format_url(url)
    print('formatted_url : ', formatted_url)
    url_dest_list = [(formatted_url, os.path.join(tmp_dir, os.path.basename(formatted_url)),
                      os.path.join(output_dir,
                                   output_prefix + '_' + exe_ts_utc.strftime('%Y-%m-%d_%H:%M') + '.asc'))]

    procs = multiprocessing.cpu_count()

    logging.info('Downloading inventory in parallel')
    download_parallel(url_dest_list, procs)
    logging.info('Downloading inventory complete')

    logging.info('Processing files in parallel')
    Parallel(n_jobs=procs)(
        delayed(process_jaxa_zip_file)(i[1], i[2], lat_min, lon_min, lat_max, lon_max, cum, output_prefix,
                                       db_adapter_config) for i in url_dest_list)
    logging.info('Processing files complete')

    logging.info('Creating sat rf gif for today')
    create_daily_gif(exe_ts_utc, output_dir, output_prefix + '_today.gif', output_prefix)

    prev_day_gif = os.path.join(output_dir, output_prefix + '_yesterday.gif')
    if not file_exists_nonempty(prev_day_gif) or exe_ts_utc.strftime('%H:%M') == '00:00':
        logging.info('Creating sat rf gif for yesterday')
        create_daily_gif(datetime_floor(exe_ts_utc, 3600 * 24) - dt.timedelta(days=1), output_dir,
                         output_prefix + '_yesterday.gif', output_prefix)

    if cum:
        logging.info('Processing cumulative')
        process_cumulative_plot(url_dest_list, start_time, end_time, output_dir, lat_min, lon_min, lat_max, lon_max)
        logging.info('Processing cumulative complete')

    # clean up temp dir
    if cleanup:
        logging.info('Cleaning up')
        shutil.rmtree(tmp_dir)
        delete_files_with_prefix(output_dir, '*.archive')


def extract_jaxa_satellite_data(start_ts_utc, end_ts_utc, output_dir, cleanup=True, cum=False, tmp_dir=None,
                                lat_min=5.722969, lon_min=79.52146, lat_max=10.06425, lon_max=82.18992,
                                output_prefix='jaxa_sat', db_adapter_config=None):
    start = datetime_floor(start_ts_utc, 3600)
    end = datetime_floor(end_ts_utc, 3600)

    login = 'rainmap:Niskur+1404'

    url0 = 'ftp://' + login + '@hokusai.eorc.jaxa.jp/realtime/txt/05_AsiaSS/YYYY/MM/DD/gsmap_nrt.YYYYMMDD.HH00.05_AsiaSS.csv.zip'
    url1 = 'ftp://' + login + '@hokusai.eorc.jaxa.jp/now/txt/05_AsiaSS/gsmap_now.YYYYMMDD.HH00_HH59.05_AsiaSS.csv.zip'

    def get_jaxa_url(ts):
        url_switch = (dt.datetime.utcnow() - ts) > dt.timedelta(hours=5)
        _url = url0 if url_switch else url1
        ph = {'YYYY': ts.strftime('%Y'),
              'MM': ts.strftime('%m'),
              'DD': ts.strftime('%d'),
              'HH': ts.strftime('%H')}
        for k, v in list(ph.items()):
            _url = _url.replace(k, v)
        return _url

    # tmp_dir = os.path.join(output_dir, 'tmp_jaxa/')
    # if not os.path.exists(tmp_dir):
    #     os.mkdir(tmp_dir)
    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp(prefix='tmp_jaxa')

    url_dest_list = []
    for timestamp in np.arange(start, end, dt.timedelta(hours=1)).astype(dt.datetime):
        url = get_jaxa_url(timestamp)
        url_dest_list.append((url, os.path.join(tmp_dir, os.path.basename(url)),
                              os.path.join(output_dir,
                                           output_prefix + '_' + timestamp.strftime('%Y-%m-%d_%H:%M') + '.asc')))

    procs = multiprocessing.cpu_count()

    logging.info('Downloading inventory in parallel')
    download_parallel(url_dest_list, procs)
    logging.info('Downloading inventory complete')

    logging.info('Processing files in parallel')
    Parallel(n_jobs=procs)(
        delayed(process_jaxa_zip_file)(i[1], i[2], lat_min, lon_min, lat_max, lon_max, cum, output_prefix,
                                       db_adapter_config) for i in url_dest_list)
    logging.info('Processing files complete')

    logging.info('Creating sat rf gif for today')
    create_daily_gif(start, output_dir, output_prefix + '_today.gif', output_prefix)

    prev_day_gif = os.path.join(output_dir, output_prefix + '_yesterday.gif')
    if not file_exists_nonempty(prev_day_gif) or start.strftime('%H:%M') == '00:00':
        logging.info('Creating sat rf gif for yesterday')
        create_daily_gif(datetime_floor(start, 3600 * 24) - dt.timedelta(days=1), output_dir,
                         output_prefix + '_yesterday.gif', output_prefix)

    if cum:
        logging.info('Processing cumulative')
        process_cumulative_plot(url_dest_list, start_ts_utc, end_ts_utc, output_dir, lat_min, lon_min, lat_max, lon_max)
        logging.info('Processing cumulative complete')

    # clean up temp dir
    if cleanup:
        logging.info('Cleaning up')
        shutil.rmtree(tmp_dir)
        delete_files_with_prefix(output_dir, '*.archive')


def test_extract_jaxa_satellite_data():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    end = dt.datetime.utcnow() - dt.timedelta(hours=6)
    start = end - dt.timedelta(hours=1)

    extract_jaxa_satellite_data(start, end, '/home/uwcc-admin/temp/jaxa', cleanup=False, tmp_dir='/home/uwcc-admin/temp/jaxa/data')


def test_extract_jaxa_satellite_data_with_db():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    end = dt.datetime.utcnow() - dt.timedelta(hours=3)
    start = end - dt.timedelta(hours=1)

    config = {
        "host": "localhost",
        "user": "test",
        "password": "password",
        "db": "testdb"
    }
    extract_jaxa_satellite_data(start, end, '/home/nira/temp1/jaxa', cleanup=False,
                                tmp_dir='/home/nira/temp1/jaxa/data',
                                db_adapter_config=config)


def test_extract_jaxa_satellite_data_d01():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    end = dt.datetime.utcnow() - dt.timedelta(hours=1)
    start = end - dt.timedelta(hours=1)

    lat_min = -3.06107
    lon_min = 71.2166
    lat_max = 18.1895
    lon_max = 90.3315

    extract_jaxa_satellite_data(start, end, '/tmp/jaxa', cleanup=False, tmp_dir='/tmp/jaxa/data',
                                lat_min=lat_min, lat_max=lat_max, lon_min=lon_min, lon_max=lon_max, output_prefix='abc')


def process_cumulative_plot(url_dest_list, start_ts_utc, end_ts_utc, output_dir, lat_min, lon_min, lat_max, lon_max):
    from_to = '%s-%s' % (start_ts_utc.strftime('%Y-%m-%d_%H:%M'), end_ts_utc.strftime('%Y-%m-%d_%H:%M'))
    cum_filename = os.path.join(output_dir, 'jaxa_sat_cum_rf_' + from_to + '.png')

    if not file_exists_nonempty(cum_filename):
        total = None
        for url_dest in url_dest_list:
            if total is None:
                total = np.genfromtxt(url_dest[2] + '.archive', dtype=float)
            else:
                total += np.genfromtxt(url_dest[2] + '.archive', dtype=float)
        title = 'Cumulative rainfall ' + from_to
        # clevs = np.concatenate(([-1, 0], np.array([pow(2, i) for i in range(0, 9)])))
        clevs_cum = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30, 50, 75, 100])
        norm_cum = colors.BoundaryNorm(boundaries=clevs_cum, ncolors=256)
        cmap = plt.get_cmap('jet')
        create_contour_plot(total, cum_filename, lat_min, lon_min, lat_max, lon_max, title, clevs=clevs_cum,
                                      cmap=cmap, norm=norm_cum)
    else:
        logging.info('%s already exits' % cum_filename)


def process_jaxa_zip_file(zip_file_path, out_file_path, lat_min, lon_min, lat_max, lon_max, archive_data=False,
                          output_prefix='jaxa_sat', db_adapter_config=None):
    sat_zip = zipfile.ZipFile(zip_file_path)
    sat = np.genfromtxt(sat_zip.open(os.path.basename(zip_file_path).replace('.zip', '')), delimiter=',', names=True)
    sat_filt = np.sort(
        sat[(sat['Lat'] <= lat_max) & (sat['Lat'] >= lat_min) & (sat['Lon'] <= lon_max) & (sat['Lon'] >= lon_min)],
        order=['Lat', 'Lon'])
    lats = np.sort(np.unique(sat_filt['Lat']))
    lons = np.sort(np.unique(sat_filt['Lon']))

    data = sat_filt['RainRate'].reshape(len(lats), len(lons))

    create_asc_file(np.flip(data, 0), lats, lons, out_file_path)

    # clevs = np.concatenate(([-1, 0], np.array([pow(2, i) for i in range(0, 9)])))
    # clevs = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30])
    # norm = colors.BoundaryNorm(boundaries=clevs, ncolors=256)
    # cmap = plt.get_cmap('jet')
    clevs = [0, 1, 2.5, 5, 7.5, 10, 15, 20, 30, 40, 50, 75, 100, 150, 200, 250, 300]
    # clevs = [0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30, 50, 75, 100]
    norm = None
    cmap = cm.s3pcpn

    ts = dt.datetime.strptime(os.path.basename(out_file_path).replace(output_prefix + '_', '').replace('.asc', ''),
                              '%Y-%m-%d_%H:%M')
    lk_ts = datetime_utc_to_lk(ts)
    title_opts = {
        'label': output_prefix + ' ' + lk_ts.strftime('%Y-%m-%d %H:%M') + ' LK\n' + ts.strftime(
            '%Y-%m-%d %H:%M') + ' UTC',
        'fontsize': 30
    }
    create_contour_plot(data, out_file_path + '.png', np.min(lats), np.min(lons), np.max(lats), np.max(lons),
                                  title_opts, clevs=clevs, cmap=cmap, norm=norm)

    if archive_data and not file_exists_nonempty(out_file_path + '.archive'):
        np.savetxt(out_file_path + '.archive', data, fmt='%g')
    else:
        logging.info('%s already exits' % (out_file_path + '.archive'))


def create_asc_file(data, lats, lons, out_file_path, cell_size=0.1, no_data_val=-99, overwrite=False):
    if not file_exists_nonempty(out_file_path) or overwrite:
        with open(out_file_path, 'wb') as out_file:
            out_file.write(('NCOLS %d\n' % len(lons)).encode())
            out_file.write(('NROWS %d\n' % len(lats)).encode())
            out_file.write(('XLLCORNER %f\n' % lons[0]).encode())
            out_file.write(('YLLCORNER %f\n' % lats[0]).encode())
            out_file.write(('CELLSIZE %f\n' % cell_size).encode())
            out_file.write(('NODATA_VALUE %d\n' % no_data_val).encode())

            np.savetxt(out_file, data, fmt='%.4f')
    else:
        logging.info('%s already exits' % out_file_path)


def create_gif(filenames, output, duration=0.5):
    images = []
    for filename in filenames:
        images.append(imageio.imread(filename))
    imageio.mimsave(output, images, duration=duration)


def file_exists_nonempty(filename):
    return os.path.exists(filename) and os.path.isfile(filename) and os.stat(filename).st_size != 0


def create_contour_plot(data, out_file_path, lat_min, lon_min, lat_max, lon_max, plot_title, basemap=None, clevs=None,
                        cmap=plt.get_cmap('Reds'), overwrite=False, norm=None, additional_changes=None, **kwargs):
    """
    create a contour plot using basemap
    :param additional_changes:
    :param norm:
    :param title_ops:
    :param cmap: color map
    :param clevs: color levels
    :param basemap: creating basemap takes time, hence you can create it outside and pass it over
    :param plot_title:
    :param data: 2D grid data
    :param out_file_path:
    :param lat_min:
    :param lon_min:
    :param lat_max:
    :param lon_max:
    :param overwrite:
    :return:
    """
    if not file_exists_nonempty(out_file_path) or overwrite:
        fig = plt.figure(figsize=(8.27, 11.69))
        # ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
        if basemap is None:
            basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max,
                              urcrnrlat=lat_max,
                              resolution='h')
        basemap.drawcoastlines()
        parallels = np.arange(math.floor(lat_min) - 1, math.ceil(lat_max) + 1, 1)
        basemap.drawparallels(parallels, labels=[1, 0, 0, 0], fontsize=10)
        meridians = np.arange(math.floor(lon_min) - 1, math.ceil(lon_max) + 1, 1)
        basemap.drawmeridians(meridians, labels=[0, 0, 0, 1], fontsize=10)

        ny = data.shape[0]
        nx = data.shape[1]
        lons, lats = basemap.makegrid(nx, ny)

        if clevs is None:
            clevs = np.arange(-1, np.max(data) + 1, 1)

        # cs = basemap.contourf(lons, lats, data, clevs, cmap=cm.s3pcpn_l, latlon=True)
        cs = basemap.contourf(lons, lats, data, clevs, cmap=cmap, latlon=True, norm=norm)

        cbar = basemap.colorbar(cs, location='bottom', pad="5%")
        cbar.set_label('mm')

        if isinstance(plot_title, str):
            plt.title(plot_title)
        elif isinstance(plot_title, dict):
            plt.title(plot_title.pop('label'), **plot_title)

        # make any additional changes to the plot
        if additional_changes is not None:
            additional_changes(plt, data, **kwargs)

        # draw_center_of_mass(data)
        # com = ndimage.measurements.center_of_mass(data)
        # plt.plot(com[1], com[0], 'ro')

        plt.draw()
        plt.savefig(out_file_path)
        # fig.savefig(out_file_path)
        plt.close()
    else:
        logging.info('%s already exists' % out_file_path)


def datetime_to_epoch(timestamp=None):
    timestamp = dt.datetime.now() if timestamp is None else timestamp
    return (timestamp - dt.datetime(1970, 1, 1)).total_seconds()


def epoch_to_datetime(epoch_time):
    return dt.datetime(1970, 1, 1) + dt.timedelta(seconds=epoch_time)


def datetime_floor(timestamp, floor_sec):
    return epoch_to_datetime(math.floor(datetime_to_epoch(timestamp) / floor_sec) * floor_sec)


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + dt.timedelta(hours=5, minutes=30 + shift_mins)


def download_parallel(url_dest_list, procs=multiprocessing.cpu_count(), retries=0, delay=60, overwrite=False,
                      secondary_dest_dir=None):
    Parallel(n_jobs=procs)(
        delayed(download_file)(i[0], i[1], retries, delay, overwrite, secondary_dest_dir) for i in url_dest_list)


def download_file(url, dest, retries=0, delay=60, overwrite=False, secondary_dest_dir=None):
    try_count = 1
    last_e = None

    def _download_file(_url, _dest):
        _f = urlopen(_url)
        with open(_dest, "wb") as _local_file:
            _local_file.write(_f.read())

    while try_count <= retries + 1:
        try:
            logging.info("Downloading %s to %s" % (url, dest))
            if secondary_dest_dir is None:
                if not overwrite and file_exists_nonempty(dest):
                    logging.info('File already exists. Skipping download!')
                else:
                    _download_file(url, dest)
                return
            else:
                secondary_file = os.path.join(secondary_dest_dir, os.path.basename(dest))
                if file_exists_nonempty(secondary_file):
                    logging.info("File available in secondary dir. Copying to the destination dir from secondary dir")
                    shutil.copyfile(secondary_file, dest)
                else:
                    logging.info("File not available in secondary dir. Downloading...")
                    _download_file(url, dest)
                    logging.info("Copying to the secondary dir")
                    shutil.copyfile(dest, secondary_file)
                return

        except (HTTPError, URLError) as e:
            logging.error(
                'Error in downloading %s Attempt %d : %s . Retrying in %d seconds' % (url, try_count, e.message, delay))
            try_count += 1
            last_e = e
            time.sleep(delay)
        except FileExistsError:
            logging.info('File was already downloaded by another process! Returning')
            return
    raise last_e


def get_output_dir(wrf_home=DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'OUTPUT'))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description='Run all stages of WRF')
    parser.add_argument('-start',
                        default=(dt.datetime.utcnow() - dt.timedelta(days=1, hours=1)).strftime('%Y-%m-%d_%H:%M'),
                        help='Start timestamp UTC with format %%Y-%%m-%%d_%%H:%%M', dest='start_ts')
    parser.add_argument('-end', default=(dt.datetime.utcnow() - dt.timedelta(hours=1)).strftime('%Y-%m-%d_%H:%M'),
                        help='End timestamp UTC with format %%Y-%%m-%%d_%%H:%%M', dest='end_ts')
    parser.add_argument('-output', default=None, help='Output directory of the images', dest='output')
    parser.add_argument('-prefix', default='jaxa_sat', help='Output prefix')
    parser.add_argument('-clean', default=1, help='Cleanup temp directory', dest='clean', type=int)
    parser.add_argument('-cum', default=0, help='Process cumulative plot', dest='cum', type=int)

    parser.add_argument('-lat_min', default=5.722969, help='Lat min', type=float)
    parser.add_argument('-lon_min', default=79.52146, help='lon min', type=float)
    parser.add_argument('-lat_max', default=10.06425, help='Lat max', type=float)
    parser.add_argument('-lon_max', default=82.18992, help='Lon max', type=float)

    parser.add_argument('-db_config', default='{}', help='DB config of to push sat data', dest='db_config')

    args = parser.parse_args()

    if args.output is None:
        output = os.path.join(get_output_dir(), 'jaxa_sat')
    else:
        output = args.output

    db_config_dict = ast.literal_eval(args.db_config)
    # adapter = ext_utils.get_curw_adapter(mysql_config=db_config_dict) if db_config_dict else None
    start_time = dt.datetime.strptime(args.start_ts, '%Y-%m-%d_%H:%M')
    start_minute = int(start_time.strftime('%M'))

    extract_jaxa_satellite_data_every_half_hr(dt.datetime.strptime(args.start_ts, '%Y-%m-%d_%H:%M'),
                                              output, cleanup=bool(args.clean), cum=bool(args.cum),
                                              lat_min=args.lat_min,
                                              lon_min=args.lon_min, lat_max=args.lat_max, lon_max=args.lon_max,
                                              output_prefix=args.prefix, db_adapter_config=db_config_dict)

