import json
import traceback
import sys
import os
from datetime import datetime, timedelta
import re

from db_adapter.logger import logger
from db_adapter.constants import COMMON_DATE_TIME_FORMAT, CURW_FCST_DATABASE, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, \
    CURW_FCST_PORT, CURW_FCST_HOST
from db_adapter.base import get_Pool
from db_adapter.curw_fcst.source import get_source_id, get_source_parameters
from db_adapter.curw_fcst.variable import get_variable_id
from db_adapter.curw_fcst.unit import get_unit_id, UnitType
from db_adapter.curw_fcst.station import get_flo2d_output_stations, StationEnum
from db_adapter.curw_fcst.timeseries import Timeseries


flo2d_stations = { }


def read_attribute_from_config_file(attribute, config, compulsory):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:
    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    elif compulsory:
        logger.error("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        logger.error("{} not specified in config file.".format(attribute))
        return None


def getUTCOffset(utcOffset, default=False):
    """
    Get timedelta instance of given UTC offset string.
    E.g. Given UTC offset string '+05:30' will return
    datetime.timedelta(hours=5, minutes=30))

    :param string utcOffset: UTC offset in format of [+/1][HH]:[MM]
    :param boolean default: If True then return 00:00 time offset on invalid format.
    Otherwise return False on invalid format.
    """
    offset_pattern = re.compile("[+-]\d\d:\d\d")
    match = offset_pattern.match(utcOffset)
    if match:
        utcOffset = match.group()
    else:
        if default:
            print("UTC_OFFSET :", utcOffset, " not in correct format. Using +00:00")
            return timedelta()
        else:
            return False

    if utcOffset[0]=="+":  # If timestamp in positive zone, add it to current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=int(offset_str[0]), minutes=int(offset_str[1]))
    if utcOffset[0]=="-":  # If timestamp in negative zone, deduct it from current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=-1 * int(offset_str[0]), minutes=-1 * int(offset_str[1]))


def get_water_level_of_channels(lines, channels=None):
    """
     Get Water Levels of given set of channels
    :param lines:
    :param channels:
    :return:
    """
    if channels is None:
        channels = []
    water_levels = { }
    for line in lines[1:]:
        if line=='\n':
            break
        v = line.split()
        if v[0] in channels:
            # Get flood level (Elevation)
            water_levels[v[0]] = v[5]
            # Get flood depth (Depth)
            # water_levels[int(v[0])] = v[2]
    return water_levels


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def extractForecastTimeseries(timeseries, extract_date, extract_time, by_day=False):
    """
    Extracted timeseries upward from given date and time
    E.g. Consider timeseries 2017-09-01 to 2017-09-03
    date: 2017-09-01 and time: 14:00:00 will extract a timeseries which contains
    values that timestamp onwards
    """
    print('LibForecastTimeseries:: extractForecastTimeseries')
    if by_day:
        extract_date_time = datetime.strptime(extract_date, '%Y-%m-%d')
    else:
        extract_date_time = datetime.strptime('%s %s' % (extract_date, extract_time), '%Y-%m-%d %H:%M:%S')

    is_date_time = isinstance(timeseries[0][0], datetime)
    new_timeseries = []
    for i, tt in enumerate(timeseries):
        tt_date_time = tt[0] if is_date_time else datetime.strptime(tt[0], '%Y-%m-%d %H:%M:%S')
        if tt_date_time >= extract_date_time:
            new_timeseries = timeseries[i:]
            break

    return new_timeseries


def save_forecast_timeseries_to_db(pool, timeseries, run_date, run_time, opts, flo2d_stations, fgt):
    print('EXTRACTFLO2DWATERLEVEL:: save_forecast_timeseries >>', opts)

    # {
    #         'tms_id'     : '',
    #         'sim_tag'    : '',
    #         'station_id' : '',
    #         'source_id'  : '',
    #         'unit_id'    : '',
    #         'variable_id': ''
    #         }

    # Convert date time with offset
    date_time = datetime.strptime('%s %s' % (run_date, run_time), COMMON_DATE_TIME_FORMAT)
    if 'utcOffset' in opts:
        date_time = date_time + opts['utcOffset']
        run_date = date_time.strftime('%Y-%m-%d')
        run_time = date_time.strftime('%H:%M:%S')

    # If there is an offset, shift by offset before proceed
    forecast_timeseries = []
    if 'utcOffset' in opts:
        print('Shift by utcOffset:', opts['utcOffset'].resolution)
        for item in timeseries:
            forecast_timeseries.append(
                    [datetime.strptime(item[0], COMMON_DATE_TIME_FORMAT) + opts['utcOffset'], item[1]])

        forecast_timeseries = extractForecastTimeseries(timeseries=forecast_timeseries, extract_date=run_date,
                extract_time=run_time)
    else:
        forecast_timeseries = extractForecastTimeseries(timeseries=timeseries, extract_date=run_date,
                extract_time=run_time)

    elementNo = opts.get('elementNo')

    tms_meta = opts.get('tms_meta')

    tms_meta['latitude'] = str(flo2d_stations.get(elementNo)[1])
    tms_meta['longitude'] = str(flo2d_stations.get(elementNo)[2])
    tms_meta['station_id'] = flo2d_stations.get(elementNo)[0]

    try:

        TS = Timeseries(pool=pool)

        tms_id = TS.get_timeseries_id_if_exists(meta_data=tms_meta)

        if tms_id is None:
            tms_id = TS.generate_timeseries_id(meta_data=tms_meta)
            tms_meta['tms_id'] = tms_id
            TS.insert_run(run_meta=tms_meta)
            TS.update_start_date(id_=tms_id, start_date=fgt)

        TS.insert_data(timeseries=forecast_timeseries, tms_id=tms_id, fgt=fgt, upsert=True)
        TS.update_latest_fgt(id_=tms_id, fgt=fgt)

    except Exception:
        logger.error("Exception occurred while pushing data to the curw_fcst database")
        traceback.print_exc()


def upload_discharges(dir_path, ts_start_date, ts_start_time, run_date, run_time):

    """
    dis_config.json
    {
      "HYCHAN_OUT_FILE": "HYCHAN.OUT",

      "utc_offset": "",

      "sim_tag": "hourly_run",

      "model": "FLO2D",
      "version": "250",

      "unit": "m3/s",
      "unit_type": "Instantaneous",

      "variable": "Discharge"
    }

    """
    try:
        config_path = os.path.join(os.getcwd(), 'extract', 'dis_config.json')
        config = json.loads(open(config_path).read())

        # flo2D related details
        HYCHAN_OUT_FILE = read_attribute_from_config_file('HYCHAN_OUT_FILE', config, True)
        output_dir = dir_path

        run_date = ts_start_date
        run_time = ts_start_time
        ts_start_date = ts_start_date
        ts_start_time = ts_start_time
        utc_offset = read_attribute_from_config_file('utc_offset', config, False)
        if utc_offset is None:
            utc_offset = ''

        # sim tag
        sim_tag = read_attribute_from_config_file('sim_tag', config, True)

        # source details
        model = read_attribute_from_config_file('model', config, True)
        version = read_attribute_from_config_file('version', config, True)

        # unit details
        unit = read_attribute_from_config_file('unit', config, True)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', config, True))

        # variable details
        variable = read_attribute_from_config_file('variable', config, True)

        hychan_out_file_path = os.path.join(output_dir, HYCHAN_OUT_FILE)

        pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, db=CURW_FCST_DATABASE, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD)

        flo2d_model_name = '{}_{}'.format(model, version)

        flo2d_source = json.loads(get_source_parameters(pool=pool, model=model, version=version))

        flo2d_stations = get_flo2d_output_stations(pool=pool, flo2d_model=StationEnum.getType(flo2d_model_name))

        source_id = get_source_id(pool=pool, model=model, version=version)

        variable_id = get_variable_id(pool=pool, variable=variable)

        unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)

        tms_meta = {
                'sim_tag'    : sim_tag,
                'model'      : model,
                'version'    : version,
                'variable'   : variable,
                'unit'       : unit,
                'unit_type'  : unit_type.value,
                'source_id'  : source_id,
                'variable_id': variable_id,
                'unit_id'    : unit_id
                }

        CHANNEL_CELL_MAP = flo2d_source["CHANNEL_CELL_MAP"]

        FLOOD_PLAIN_CELL_MAP = flo2d_source["FLOOD_PLAIN_CELL_MAP"]

        ELEMENT_NUMBERS = CHANNEL_CELL_MAP.keys()
        FLOOD_ELEMENT_NUMBERS = FLOOD_PLAIN_CELL_MAP.keys()
        SERIES_LENGTH = 0
        MISSING_VALUE = -999

        utcOffset = getUTCOffset(utc_offset, default=True)

        fgt = (datetime.now() + timedelta(hours=5, minutes=30)).strftime(COMMON_DATE_TIME_FORMAT)

        print('Extract Discharge Result of FLO2D on', run_date, '@', run_time, 'with Base time of', ts_start_date,
                '@', ts_start_time)

        # Check HYCHAN.OUT file exists
        if not os.path.exists(hychan_out_file_path):
            print('Unable to find file : ', hychan_out_file_path)
            traceback.print_exc()

        #####################################
        # Calculate the size of time series #
        #####################################
        bufsize = 65536
        with open(hychan_out_file_path) as infile:
            isWaterLevelLines = False
            isCounting = False
            countSeriesSize = 0  # HACK: When it comes to the end of file, unable to detect end of time series
            while True:
                lines = infile.readlines(bufsize)
                if not lines or SERIES_LENGTH:
                    break
                for line in lines:
                    if line.startswith('CHANNEL HYDROGRAPH FOR ELEMENT NO:', 5):
                        isWaterLevelLines = True
                    elif isWaterLevelLines:
                        cols = line.split()
                        if len(cols) > 0 and cols[0].replace('.', '', 1).isdigit():
                            countSeriesSize += 1
                            isCounting = True
                        elif isWaterLevelLines and isCounting:
                            SERIES_LENGTH = countSeriesSize
                            break

        print('Series Length is :', SERIES_LENGTH)
        bufsize = 65536
        #################################################################
        # Extract Channel Discharge from HYCHAN.OUT file   #
        #################################################################
        print('Extract Channel Discharge Result of FLO2D (HYCHAN.OUT) on', run_date, '@', run_time,
                'with Base time of',
                ts_start_date, '@', ts_start_time)
        with open(hychan_out_file_path) as infile:
            isWaterLevelLines = False
            isSeriesComplete = False
            waterLevelLines = []
            seriesSize = 0  # HACK: When it comes to the end of file, unable to detect end of time series
            while True:
                lines = infile.readlines(bufsize)
                if not lines:
                    break
                for line in lines:
                    if line.startswith('CHANNEL HYDROGRAPH FOR ELEMENT NO:', 5):
                        seriesSize = 0
                        elementNo = line.split()[5]

                        if elementNo in ELEMENT_NUMBERS:
                            isWaterLevelLines = True
                            waterLevelLines.append(line)
                        else:
                            isWaterLevelLines = False

                    elif isWaterLevelLines:
                        cols = line.split()
                        if len(cols) > 0 and isfloat(cols[0]):
                            seriesSize += 1
                            waterLevelLines.append(line)

                            if seriesSize==SERIES_LENGTH:
                                isSeriesComplete = True

                    if isSeriesComplete:
                        baseTime = datetime.strptime('%s %s' % (ts_start_date, ts_start_time), '%Y-%m-%d %H:%M:%S')
                        timeseries = []
                        elementNo = waterLevelLines[0].split()[5]
                        # print('Extracted Cell No', elementNo, CHANNEL_CELL_MAP[elementNo])
                        for ts in waterLevelLines[1:]:
                            v = ts.split()
                            if len(v) < 1:
                                continue
                            # Get Discharge
                            value = v[4]
                            if not isfloat(value):
                                value = MISSING_VALUE
                                continue  # If value is not present, skip
                            if value=='NaN':
                                continue  # If value is NaN, skip
                            timeStep = float(v[0])
                            currentStepTime = baseTime + timedelta(hours=timeStep)
                            dateAndTime = currentStepTime.strftime("%Y-%m-%d %H:%M:%S")
                            timeseries.append([dateAndTime, value])

                        # Save Forecast values into Database
                        opts = {
                                'elementNo': elementNo,
                                'tms_meta' : tms_meta
                                }
                        # print('>>>>>', opts)
                        if utcOffset!=timedelta():
                            opts['utcOffset'] = utcOffset

                        # Push timeseries to database
                        save_forecast_timeseries_to_db(pool=pool, timeseries=timeseries,
                                run_date=run_date, run_time=run_time, opts=opts, flo2d_stations=flo2d_stations, fgt=fgt)

                        isWaterLevelLines = False
                        isSeriesComplete = False
                        waterLevelLines = []
                # -- END for loop
            # -- END while loop

    except Exception as e:
        traceback.print_exc()
    finally:
        logger.info("Process finished.")
        print("Process finished.")