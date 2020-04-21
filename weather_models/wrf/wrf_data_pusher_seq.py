#!/home/curw/git/curw_wrf_data_pusher/venv/bin/python3
# extract a given wrf output, for a given run date
import traceback
from netCDF4 import Dataset
import numpy as np
import os
import json
from datetime import datetime, timedelta
import time
import sys
import getopt

from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import StationEnum, get_station_id, add_station, get_wrf_stations
from db_adapter.curw_fcst.timeseries import Timeseries
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.constants import (
    CURW_FCST_DATABASE, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, CURW_FCST_PORT,
    CURW_FCST_HOST,
)

from db_adapter.logger import logger

SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]

wrf_v3_stations = {}

email_content = {}


def usage():
    usageText = """
    Usage: ./wrf_data_pusher.py -c [config_file_path] -d [wrf_root_directory] -r [gfs_run] -H [gfs_data_hour]
    -s [wrf_system] -D [date] 

    -h  --help          Show usage
    -c  --config        Config file name or path. e.g: "wrf_config.json"
    -d  --dir           WRF root directory. e.g.: "/mnt/disks/wrf_nfs/wrf"
    -r  --run           GFS run. e.g: d0 (for yesterday gfs data), d1 (for today gfs data) 
    -H  --hour          GFS data hour. e.g: 00,06,12,18
    -s  --wrf_system    WRF System. e.g.: A,C,E,SE
    -D  --date          Run date. e.g.: 2019-10-07 (date of the directory containing the wrf output to be extracted)


    """
    print(usageText)


def read_attribute_from_config_file(attribute, config):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :return:
    """

    if attribute in config and (config[attribute] != ""):
        return config[attribute]
    else:
        msg = "{} not specified in config file.".format(attribute)
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        exit(1)


def get_per_time_slot_values(prcp):
    per_interval_prcp = (prcp[1:] - prcp[:-1])
    return per_interval_prcp


def get_file_last_modified_time(file_path):
    # returns local time (UTC + 5 30)
    modified_time = time.gmtime(os.path.getmtime(file_path) + 19800)

    return time.strftime('%Y-%m-%d %H:%M:%S', modified_time)


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


def update_latest_fgt(ts, tms_id, fgt):
    try:
        ts.update_latest_fgt(id_=tms_id, fgt=fgt)
    except Exception:
        try:
            time.sleep(5)
            ts.update_latest_fgt(id_=tms_id, fgt=fgt)
        except Exception:
            msg = "Updating fgt {} for tms_id {} failed.".format(fgt, tms_id)
            logger.error(msg)
            traceback.print_exc()
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg


def gen_rfields(config_file_path, wrf_root_directory, gfs_run, gfs_data_hour, wrf_system, date):
    os.system("./gen_rfields.sh {} {} {} {} {} {}".format(config_file_path, wrf_root_directory, gfs_run,
                                                          gfs_data_hour, wrf_system, date))


def gen_hybrid_rfields(config_file_path, wrf_root_directory, gfs_run, gfs_data_hour, wrf_systems, date):
    os.system("./gen_active_stations_rfields.sh {} {} {} {} {} {}".
              format(config_file_path, wrf_root_directory, gfs_run, gfs_data_hour, wrf_systems, date))

    os.system("./gen_hybrid_wrf_rfields.sh {} {} {} {} {} {}".
              format(config_file_path, wrf_root_directory, gfs_run, gfs_data_hour, wrf_systems, date))


def push_rainfall_to_db(ts, ts_data, tms_id, fgt):
    """

    :param ts: timeseries class instance
    :param ts_data: timeseries
    :return:
    """

    try:
        ts.insert_formatted_data(ts_data, True)  # upsert True
        update_latest_fgt(ts, tms_id, fgt)
    except Exception:
        try:
            time.sleep(5)
            ts.insert_formatted_data(ts_data, True)  # upsert True
            update_latest_fgt(ts, tms_id, fgt)
        except Exception:
            msg = "Inserting the timseseries for tms_id {} and fgt {} failed.".format(ts_data[0][0], ts_data[0][2])
            logger.error(msg)
            traceback.print_exc()
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg


def read_netcdf_file(pool, rainnc_net_cdf_file_path, tms_meta):
    """

    :param pool: database connection pool
    :param rainnc_net_cdf_file_path:
    :param source_id:
    :param variable_id:
    :param unit_id:
    :param tms_meta:
    :return:

    rainc_unit_info:  mm
    lat_unit_info:  degree_north
    time_unit_info:  minutes since 2019-04-02T18:00:00
    """
    if not os.path.exists(rainnc_net_cdf_file_path):
        msg = 'no rainnc netcdf :: {}'.format(rainnc_net_cdf_file_path)
        logger.warning(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
    else:

        try:
            """
            RAINNC netcdf data extraction

            """
            fgt = get_file_last_modified_time(rainnc_net_cdf_file_path)

            nnc_fid = Dataset(rainnc_net_cdf_file_path, mode='r')

            time_unit_info = nnc_fid.variables['XTIME'].description

            time_unit_info_list = time_unit_info.split('since ')

            lats = nnc_fid.variables['XLAT'][0, :, 0]
            lons = nnc_fid.variables['XLONG'][0, 0, :]

            lon_min = lons[0].item()
            lat_min = lats[0].item()
            lon_max = lons[-1].item()
            lat_max = lats[-1].item()

            lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
            lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

            rainnc = nnc_fid.variables['RAINNC'][:, lat_inds[0], lon_inds[0]]

            times = nnc_fid.variables['XTIME'][:]

            start_date = fgt
            end_date = fgt

            nnc_fid.close()

            diff = get_per_time_slot_values(rainnc)

            if len(diff) < 1:
                msg = "The timeseies of the netcdf at {} is empty".format(
                    rainnc_net_cdf_file_path)
                email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
                exit(1)

            width = len(lons)
            height = len(lats)

            ts = Timeseries(pool)

            for y in range(height):
                for x in range(width):

                    lat = float('%.6f' % lats[y])
                    lon = float('%.6f' % lons[x])

                    tms_meta['latitude'] = str(lat)
                    tms_meta['longitude'] = str(lon)

                    station_prefix = 'wrf_{}_{}'.format(lat, lon)

                    station_id = wrf_v3_stations.get(station_prefix)

                    if station_id is None:
                        add_station(pool=pool, name=station_prefix, latitude=lat, longitude=lon,
                                    description="WRF point", station_type=StationEnum.WRF)
                        station_id = get_station_id(pool=pool, latitude=lat, longitude=lon,
                                                    station_type=StationEnum.WRF)

                    tms_id = ts.get_timeseries_id_if_exists(tms_meta)

                    if tms_id is None:
                        tms_id = ts.generate_timeseries_id(tms_meta)

                        run_meta = {
                            'tms_id': tms_id,
                            'sim_tag': tms_meta['sim_tag'],
                            'start_date': start_date,
                            'end_date': end_date,
                            'station_id': station_id,
                            'source_id': tms_meta['source_id'],
                            'unit_id': tms_meta['unit_id'],
                            'variable_id': tms_meta['variable_id']
                        }

                        try:
                            ts.insert_run(run_meta)
                        except Exception:
                            time.sleep(5)
                            try:
                                ts.insert_run(run_meta)
                            except Exception:
                                msg = "Exception occurred while inserting run entry {}".format(run_meta)
                                logger.error(msg)
                                traceback.print_exc()
                                email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg

                    data_list = []
                    # generate timeseries for each station
                    for i in range(len(diff)):
                        ts_time = datetime.strptime(time_unit_info_list[1], '%Y-%m-%d %H:%M:%S') + timedelta(
                            minutes=times[i + 1].item())
                        t = datetime_utc_to_lk(ts_time, shift_mins=0)
                        data_list.append([tms_id, t.strftime('%Y-%m-%d %H:%M:00'), fgt, float('%.3f' % diff[i, y, x])])

                    if len(data_list) > 0:
                        push_rainfall_to_db(ts=ts, ts_data=data_list, tms_id=tms_id, fgt=fgt)
        except Exception as e:
            msg = "netcdf file at {} reading error.".format(rainnc_net_cdf_file_path)
            logger.error(msg)
            traceback.print_exc()
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg


def extract_wrf_data(config_data, tms_meta):
    source_name = "{}_{}".format(config_data['model'], config_data['wrf_system'])

    source_id = None

    try:
        source_id = get_source_id(pool=pool, model=source_name, version=tms_meta['version'])
    except Exception:
        try:
            time.sleep(3)
            source_id = get_source_id(pool=pool, model=source_name, version=tms_meta['version'])
        except Exception:
            msg = "Exception occurred while loading source meta data for WRF_{} from database.".format(
                config_data['wrf_system'])
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            traceback.print_exc()

    if source_id is None:
        try:
            add_source(pool=pool, model=source_name, version=tms_meta['version'])
            source_id = get_source_id(pool=pool, model=source_name, version=tms_meta['version'])
        except Exception:
            msg = "Exception occurred while addding new source {} {} to database.".format(source_name,
                                                                                          tms_meta['version'])
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            traceback.print_exc()

    tms_meta['model'] = source_name
    tms_meta['source_id'] = source_id

    # Buckets/wrf_nfs/wrf/4.0/d1/00/2019-10-04/SE/d03_RAINNC.nc

    output_dir = os.path.join(config_data['wrf_dir'], config_data['version'], config_data['gfs_run'],
                              config_data['gfs_data_hour'], config_data['date'], 'output',
                              config_data['wrf_type'], config_data['wrf_system'])

    rainnc_net_cdf_file = 'd03_RAINNC.nc'

    rainnc_net_cdf_file_path = os.path.join(output_dir, rainnc_net_cdf_file)

    read_netcdf_file(pool=pool, rainnc_net_cdf_file_path=rainnc_net_cdf_file_path, tms_meta=tms_meta)

    if not bool(email_content):
        gen_rfields(config_file_path=config_data['config_path'], wrf_root_directory=config_data['wrf_dir'],
                    gfs_run=config_data['gfs_run'], gfs_data_hour=config_data['gfs_data_hour'],
                    wrf_system=config_data['wrf_system'], date=config_data['date'])

        gen_hybrid_rfields(config_file_path=config_data['config_path'], wrf_root_directory=config_data['wrf_dir'],
                           gfs_run=config_data['gfs_run'], gfs_data_hour=config_data['gfs_data_hour'],
                           wrf_systems=config_data['wrf_system'], date=config_data['date'])


if __name__ == "__main__":

    """
    Config.json 
    {
      "version": "4.0",
      "wrf_type": "dwrf",

      "model": "WRF",

      "unit": "mm",
      "unit_type": "Accumulative",

      "variable": "Precipitation"
    }



    /wrf_nfs/wrf/4.0/d0/18/A/2019-07-30/d03_RAINNC.nc

    tms_meta = {
                    'sim_tag'       : sim_tag,
                    'latitude'      : latitude,
                    'longitude'     : longitude,
                    'model'         : model,
                    'version'       : version,
                    'variable'      : variable,
                    'unit'          : unit,
                    'unit_type'     : unit_type
                    }
    """
    pool = None
    config_data = {}

    try:

        config_path = None
        wrf_dir = None
        gfs_run = None
        gfs_data_hour = None
        wrf_system = None
        date = None

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:c:d:r:H:s:D:a:",
                                       ["help", "config=", "dir=", "run=", "hour=", "wrf_system=", "date=", "sim_tag="])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-c", "--config"):
                config_path = arg.strip()
            elif opt in ("-d", "--dir"):
                wrf_dir = arg.strip()
            elif opt in ("-r", "--run"):
                gfs_run = arg.strip()
            elif opt in ("-H", "--hour"):
                gfs_data_hour = arg.strip()
            elif opt in ("-s", "--wrf_system"):
                wrf_system = arg.strip()
            elif opt in ("-D", "--date"):
                date = arg.strip()
            elif opt in ("-a", "--sim_tag"):
                sim_tag = arg.strip()

        if config_path is None:
            msg = "Config file name is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)

        config = json.loads(open(config_path).read())

        # source details
        if wrf_dir is None:
            msg = "WRF root directory is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)
        if gfs_run is None:
            msg = "GFS run is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)
        if gfs_data_hour is None:
            msg = "GFS data hour is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)
        if wrf_system is None:
            msg = "WRF system is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)
        model = read_attribute_from_config_file('model', config)
        version = read_attribute_from_config_file('version', config)
        wrf_type = read_attribute_from_config_file('wrf_type', config)

        # sim_tag
        print('************* User defined sim_tag : ',sim_tag)
        if sim_tag is None:
            sim_tag_prefix = ''
            if wrf_type != 'wrf':
                sim_tag_prefix = wrf_type + "_"
            sim_tag = sim_tag_prefix + 'gfs_{}_{}'.format(gfs_run, gfs_data_hour)
        print('************* Final defined sim_tag : ', sim_tag)
        # unit details
        unit = read_attribute_from_config_file('unit', config)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', config))

        # variable details
        variable = read_attribute_from_config_file('variable', config)

        if date is None:
            msg = "Run date is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)

        pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                        db=CURW_FCST_DATABASE)

        try:
            wrf_v3_stations = get_wrf_stations(pool)

            variable_id = get_variable_id(pool=pool, variable=variable)
            unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)
        except Exception:
            msg = "Exception occurred while loading common metadata from database."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            traceback.print_exc()
            exit(1)

        tms_meta = {
            'sim_tag': sim_tag,
            'version': version,
            'variable': variable,
            'unit': unit,
            'unit_type': unit_type.value,
            'variable_id': variable_id,
            'unit_id': unit_id
        }

        config_data = {
            'model': model,
            'version': version,
            'date': date,
            'wrf_dir': wrf_dir,
            'gfs_run': gfs_run,
            'gfs_data_hour': gfs_data_hour,
            'wrf_system': wrf_system,
            'config_path': config_path,
            'wrf_type': wrf_type
        }

        extract_wrf_data(config_data, tms_meta)

    except Exception as e:
        msg = 'Config data loading error.'
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        traceback.print_exc()
    finally:
        if pool is not None:
            destroy_Pool(pool)
        print("{} ::: Sequential WRF data extraction Process  \n:::Email Content {}  \n::: Config details"
              .format(datetime.now(), json.dumps(email_content), json.dumps(config_data)))

