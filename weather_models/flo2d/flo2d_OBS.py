#!/home/curw/event_sim_utils/venv/bin/python3

import sys
import getopt
import os
import traceback
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv

from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.curw_sim.constants import FLO2D_250, FLO2D_150, FLO2D_150_V2
from db_adapter.curw_sim.timeseries.discharge import Timeseries as DTimeseries
from db_adapter.curw_sim.timeseries.waterlevel import Timeseries as WLTimeseries
from db_adapter.curw_sim.timeseries import MethodEnum

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
ROOT_DIR = '/home/curw/event_sim_utils'


def check_time_format(time, model):
    # hourly timeseries
    try:
        time = datetime.strptime(time, DATE_TIME_FORMAT)

        if time.strftime('%S') != '00':
            print("Seconds should be always 00")
            exit(1)
        # if model=="flo2d_250" and time.strftime('%M') not in ('05', '10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '00'):
        #     print("Minutes should be multiple of 5 fro flo2d_250")
        #     exit(1)
        if model in ("flo2d_250", "flo2d_150", "flo2d_150_v2") and time.strftime('%M') != '00':
            print("Minutes should be always 00")
            exit(1)

        return True
    except Exception:
        traceback.print_exc()
        print("Time {} is not in proper format".format(time))
        exit(1)


def calculate_hanwella_discharge(hanwella_wl_ts):
    discharge_ts = []
    for i in range(len(hanwella_wl_ts)):
        wl = float(hanwella_wl_ts[i][1])
        discharge = 26.1131 * (wl ** 1.73499)
        discharge_ts.append([hanwella_wl_ts[i][0], '%.3f' % discharge])

    return discharge_ts


def calculate_glencourse_discharge(glencourse_wl_ts):
    """Q = 41.904 (H – 7.65)^1.518 (H&lt;=16.0 m AMSL)
Q = 21.323 (H – 7.71)^1.838 (H&gt;16.0 m AMSL)"""
    discharge_ts = []
    for i in range(len(glencourse_wl_ts)):
        wl = float(glencourse_wl_ts[i][1])
        if wl <= 16:
            discharge = 41.904 * ((wl - 7.65) ** 1.518)
        else:
            discharge = 21.323 * ((wl - 7.71) ** 1.838)
        discharge_ts.append([glencourse_wl_ts[i][0], '%.3f' % discharge])

    return discharge_ts


def update_discharge_obs(curw_sim_pool, flo2d_model, method, timestep, start_time, end_time):
    try:

        discharge_TS = DTimeseries(pool=curw_sim_pool)
        waterlevel_TS = WLTimeseries(pool=curw_sim_pool)

        # [station_name,latitude,longitude,target]
        extract_stations = read_csv('grids/discharge_stations/flo2d_stations.csv')
        extract_stations_dict = {}  # keys: target_model , value: [latitude, longitude, station_name]
        # keys: station_name , value: [latitude, longitude, target_model]

        for obs_index in range(len(extract_stations)):
            extract_stations_dict[extract_stations[obs_index][3]] = [extract_stations[obs_index][1],
                                                                     extract_stations[obs_index][2],
                                                                     extract_stations[obs_index][0]]

        station_name = extract_stations_dict.get(flo2d_model)[2]
        meta_data = {
            'latitude': float('%.6f' % float(extract_stations_dict.get(flo2d_model)[0])),
            'longitude': float('%.6f' % float(extract_stations_dict.get(flo2d_model)[1])),
            'model': flo2d_model, 'method': method,
            'grid_id': 'discharge_{}'.format(station_name)
        }

        wl_meta_data = {
            'latitude': float('%.6f' % float(extract_stations_dict.get(flo2d_model)[0])),
            'longitude': float('%.6f' % float(extract_stations_dict.get(flo2d_model)[1])),
            'model': flo2d_model, 'method': method,
            'grid_id': 'waterlevel_{}'.format(station_name)
        }

        tms_id = discharge_TS.get_timeseries_id_if_exists(meta_data=meta_data)
        wl_tms_id = waterlevel_TS.get_timeseries_id_if_exists(meta_data=wl_meta_data)

        if wl_tms_id is None:
            print("Warning!!! {} waterlevel timeseries doesn't exist.".format(station_name))
            exit(1)

        timeseries = []

        if tms_id is None:
            tms_id = discharge_TS.generate_timeseries_id(meta_data=meta_data)
            meta_data['id'] = tms_id
            discharge_TS.insert_run(meta_data=meta_data)

        wl_timeseries = waterlevel_TS.get_timeseries(id_=wl_tms_id, start_date=start_time, end_date=end_time)

        estimated_discharge_ts = []

        if station_name == 'hanwella':
            estimated_discharge_ts = calculate_hanwella_discharge(wl_timeseries)
        elif station_name == 'glencourse':
            estimated_discharge_ts = calculate_glencourse_discharge(wl_timeseries)

        if estimated_discharge_ts is not None and len(estimated_discharge_ts) > 0:
            discharge_TS.insert_data(timeseries=estimated_discharge_ts, tms_id=tms_id, upsert=True)

    except Exception as e:
        traceback.print_exc()


def usage():
    usageText = """
    --------------------------------------------------
    Populate discharge Flo2D 250, 150 & 150_v2 :: OBS
    --------------------------------------------------

    Usage: ./discharge/flo2d_OBS.py [-m flo2d_XXX][-s "YYYY-MM-DD HH:MM:SS"] [-e "YYYY-MM-DD HH:MM:SS"]

    -h  --help          Show usage
    -m  --flo2d_model   FLO2D model (e.g. flo2d_250, flo2d_150). Default is flo2d_250.
    -s  --start_time    Discharge timeseries start time (e.g: "2019-06-05 00:00:00"). Default is 23:00:00, 3 days before today.
    -e  --end_time      Discharge timeseries end time (e.g: "2019-06-05 23:00:00"). Default is 23:00:00, tomorrow.
    """
    print(usageText)


if __name__ == "__main__":

    set_db_config_file_path(os.path.join(ROOT_DIR, 'db_adapter_config.json'))

    try:
        start_time = None
        end_time = None
        flo2d_model = None
        method = "OBS"

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:s:e:",
                                       ["help", "flo2d_model=", "start_time=", "end_time="])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-m", "--flo2d_model"):
                flo2d_model = arg.strip()
            elif opt in ("-s", "--start_time"):
                start_time = arg.strip()
            elif opt in ("-e", "--end_time"):
                end_time = arg.strip()

        if flo2d_model is None:
            print("Flo2d model is not specified.")
            exit(1)
        elif flo2d_model not in ("flo2d_250", "flo2d_150", "flo2d_150_v2"):
            print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\" or \"flo2d_150_v2\"")
            exit(1)

        if start_time is None:
            start_time = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d 23:00:00')
        else:
            check_time_format(time=start_time, model=flo2d_model)

        if end_time is None:
            end_time = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 23:00:00')
        else:
            check_time_format(time=end_time, model=flo2d_model)

        if flo2d_model == FLO2D_250:
            timestep = 60
        elif flo2d_model == FLO2D_150:
            timestep = 60
        elif flo2d_model == FLO2D_150_V2:
            timestep = 60

        curw_sim_pool = get_Pool(host=con_params.CURW_SIM_HOST, user=con_params.CURW_SIM_USERNAME,
                                 password=con_params.CURW_SIM_PASSWORD, port=con_params.CURW_SIM_PORT,
                                 db=con_params.CURW_SIM_DATABASE)

        print("{} : ####### Insert obs waterlevel series for {}.".format(datetime.now(), flo2d_model))
        os.system("./waterlevel/flo2d_OBS.py -m {} -s \"{}\" -e \"{}\""
                  .format(flo2d_model, start_time, end_time))

        print("{} : ####### Insert obs discharge series for {}.".format(datetime.now(), flo2d_model))
        update_discharge_obs(curw_sim_pool=curw_sim_pool, flo2d_model=flo2d_model, method=method, timestep=timestep,
                             start_time=start_time, end_time=end_time)

    except Exception as e:
        traceback.print_exc()
    finally:
        destroy_Pool(pool=curw_sim_pool)
        print("Process finished.")


