#!"D:\curw_flo2d_data_manager\venv\Scripts\python.exe"
import pymysql
from datetime import datetime, timedelta
import traceback
import json
import os
import sys
import getopt

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
ROOT_DIRECTORY = r"D:\flo2d_hourly"

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
# from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_HOST, CURW_SIM_PASSWORD, CURW_SIM_PORT, CURW_SIM_USERNAME
from db_adapter.curw_sim.grids import get_flo2d_initial_conditions


def save_metadata_to_file(input_filepath, metadata):
    metadata_filepath = os.path.join(os.path.dirname(input_filepath), "run_meta.json")

    updated_metadata = {}
    try:
        existing_metadata = json.loads(open(metadata_filepath).read())
        updated_metadata = existing_metadata
    except FileNotFoundError as eFNFE:
        pass

    for key in metadata.keys():
        updated_metadata[key] = metadata[key]

    with open(metadata_filepath, 'w') as outfile:
        json.dump(updated_metadata, outfile)


def write_file_to_file(file_name, file_content):
    with open(file_name, 'w+') as f:
        f.write(file_content)


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def append_to_file(file_name, data):
    with open(file_name, 'a+') as f:
        f.write('\n')
        f.write('\n'.join(data))


def append_file_to_file(file_name, file_content):
    with open(file_name, 'a+') as f:
        f.write('\n')
        f.write(file_content)


def makedir_if_not_exist_given_filepath(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            pass


def getWL(connection, wl_id, start_date, end_date):
    with connection.cursor() as cursor1:
        cursor1.callproc('getWL', (wl_id, start_date, end_date))
        result = cursor1.fetchone()
        if result is not None:
            return result.get('value')
        else:
            return None


def read_attribute_from_config_file(attribute, config, compulsory=False):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:

    """
    if attribute in config and (config[attribute] != ""):
        return config[attribute]
    elif compulsory:
        print("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        print("{} not specified in config file.".format(attribute))
        return None


def check_time_format(time):
    try:
        time = datetime.strptime(time, DATE_TIME_FORMAT)

        if time.strftime('%S') != '00':
            print("Seconds should be always 00")
            exit(1)
        if time.strftime('%M') != '00':
            print("Minutes should be always 00")
            exit(1)

        return True
    except Exception:
        print("Time {} is not in proper format".format(time))
        exit(1)


def prepare_chan(chan_file_path, start, flo2d_model):
    flo2d_version = flo2d_model.split('_')[1]

    try:

        curw_sim_pool = get_Pool(host=con_params.CURW_SIM_HOST, user=con_params.CURW_SIM_USERNAME,
                                 password=con_params.CURW_SIM_PASSWORD, port=con_params.CURW_SIM_PORT,
                                 db=con_params.CURW_SIM_DATABASE)

        curw_obs_pool = get_Pool(host=con_params.CURW_OBS_HOST, user=con_params.CURW_OBS_USERNAME,
                                 password=con_params.CURW_OBS_PASSWORD, port=con_params.CURW_OBS_PORT,
                                 db=con_params.CURW_OBS_DATABASE)
        obs_connection = curw_obs_pool.connection()

        # retrieve initial conditions from database
        initial_conditions = get_flo2d_initial_conditions(pool=curw_sim_pool, flo2d_model=flo2d_model)

        # chan head
        head_file = open(os.path.join(ROOT_DIRECTORY, "chan", "chan_{}_head.dat".format(flo2d_version)), "r")
        head = head_file.read()
        head_file.close()
        write_file_to_file(chan_file_path, file_content=head)

        # chan body
        chan_processed_body = []

        body_file_name = os.path.join(ROOT_DIRECTORY,  "chan", "chan_{}_body.dat".format(flo2d_version))
        chan_body = [line.rstrip('\n') for line in open(body_file_name, "r")]

        i = 0
        while i < len(chan_body):
            up_strm = chan_body[i].split()[0]
            up_strm_default = chan_body[i].split()[1]
            dwn_strm = chan_body[i + 1].split()[0]
            dwn_strm_default = chan_body[i + 1].split()[1]
            grid_id = "{}_{}_{}".format(flo2d_model, up_strm, dwn_strm)
            wl_id = initial_conditions.get(grid_id)[2]
            wl_id_dwn_strm = initial_conditions.get(grid_id)[3]
            offset = (datetime.strptime(start, DATE_TIME_FORMAT) + timedelta(hours=2)).strftime(DATE_TIME_FORMAT)
            water_level = getWL(connection=obs_connection, wl_id=wl_id, start_date=start, end_date=offset)
            water_level_dwn_strm = getWL(connection=obs_connection, wl_id=wl_id_dwn_strm, start_date=start,
                                         end_date=offset)

            if wl_id_dwn_strm is None:
                if water_level is None:
                    chan_processed_body.append("{}{}".format(up_strm.ljust(6), (str(up_strm_default)).rjust(6)))
                    chan_processed_body.append("{}{}".format(dwn_strm.ljust(6), (str(dwn_strm_default)).rjust(6)))
                else:
                    chan_processed_body.append("{}{}".format(up_strm.ljust(6), (str(water_level)).rjust(6)))
                    chan_processed_body.append("{}{}".format(dwn_strm.ljust(6), (str(water_level)).rjust(6)))
            else:
                if water_level is None:
                    chan_processed_body.append("{}{}".format(up_strm.ljust(6), (str(up_strm_default)).rjust(6)))
                else:
                    chan_processed_body.append("{}{}".format(up_strm.ljust(6), (str(water_level)).rjust(6)))
                if water_level_dwn_strm is None:
                    chan_processed_body.append("{}{}".format(dwn_strm.ljust(6), (str(dwn_strm_default)).rjust(6)))
                else:
                    chan_processed_body.append("{}{}".format(dwn_strm.ljust(6), (str(water_level_dwn_strm)).rjust(6)))

            i += 2

        append_to_file(chan_file_path, data=chan_processed_body)

        # chan tail
        tail_file = open(os.path.join(ROOT_DIRECTORY,  "chan", "chan_{}_tail.dat".format(flo2d_version)), "r")
        tail = tail_file.read()
        tail_file.close()
        append_file_to_file(chan_file_path, file_content=tail)

    except Exception as e:
        print(traceback.print_exc())
    finally:
        destroy_Pool(curw_sim_pool)
        destroy_Pool(curw_obs_pool)
        print("Chan generated")


def create_dir_if_not_exists(path):
    """
    create directory(if needed recursively) or paths
    :param path: string : directory path
    :return: string
    """
    if not os.path.exists(path):
        os.makedirs(path)

    return path


def usage():
    usageText = """
    ------------------------------------------
    Prepare CHAN for Flo2D 250 & Flo2D 150
    ------------------------------------------
    Usage: .\input\chan\gen_chan.py [-m flo2d_XXX] [-s "YYYY-MM-DD HH:MM:SS"] [-d "directory_path"]

    -h  --help          Show usage
    -m  --model         FLO2D model (e.g. flo2d_250, flo2d_150). Default is flo2d_250.
    -s  --start_time    Chan start time (e.g: "2019-06-05 00:00:00"). Default is 00:00:00, 2 days before today.
    -d  --dir           Chan file generation location (e.g: "C:\\udp_150\\2019-09-23")
    """
    print(usageText)


def create_chan(output_dir, start_time, flo2d_model):
    file_name = 'CHAN.DAT'

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:m:s:d:",
                                   ["help", "flo2d_model=", "start_time=", "dir="])
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
        elif opt in ("-d", "--dir"):
            output_dir = arg.strip()

    if flo2d_model is None:
        flo2d_model = "flo2d_250"
    elif flo2d_model not in ("flo2d_250", "flo2d_150"):
        print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\"")
        exit(1)

    if start_time is None:
        start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d 00:00:00')
    else:
        check_time_format(time=start_time)

    if output_dir is not None:
        chan_file_path = os.path.join(output_dir, file_name)
    else:
        chan_file_path = os.path.join(r"D:\chan",
                                      'CHAN_{}_{}.DAT'.format(flo2d_model, start_time).replace(' ', '_').replace(
                                          ':', '-'))

    makedir_if_not_exist_given_filepath(chan_file_path)

    if not os.path.isfile(chan_file_path):
        print("{} start preparing chan".format(datetime.now()))
        prepare_chan(chan_file_path, start=start_time, flo2d_model=flo2d_model)
        metadata = {
            "chan": {
                "tag": "from curw_sim database",
                "model": flo2d_model
            }
        }
        save_metadata_to_file(input_filepath=chan_file_path, metadata=metadata)
        print("{} completed preparing chan".format(datetime.now()))
    else:
        print('Chan file already in path : ', chan_file_path)


if __name__ == "__main__":

    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        start_time = None
        flo2d_model = None
        output_dir = None
        file_name = 'CHAN.DAT'

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:s:d:",
                                       ["help", "flo2d_model=", "start_time=", "dir="])
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
            elif opt in ("-d", "--dir"):
                output_dir = arg.strip()

        if flo2d_model is None:
            flo2d_model = "flo2d_250"
        elif flo2d_model not in ("flo2d_250", "flo2d_150"):
            print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\"")
            exit(1)

        if start_time is None:
            start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=start_time)

        if output_dir is not None:
            chan_file_path = os.path.join(output_dir, file_name)
        else:
            chan_file_path = os.path.join(r"D:\chan",
                                          'CHAN_{}_{}.DAT'.format(flo2d_model, start_time).replace(' ', '_').replace(
                                              ':', '-'))

        makedir_if_not_exist_given_filepath(chan_file_path)

        if not os.path.isfile(chan_file_path):
            print("{} start preparing chan".format(datetime.now()))
            prepare_chan(chan_file_path, start=start_time, flo2d_model=flo2d_model)
            metadata = {
                "chan": {
                    "tag": "from curw_sim database",
                    "model": flo2d_model
                }
            }
            save_metadata_to_file(input_filepath=chan_file_path, metadata=metadata)
            print("{} completed preparing chan".format(datetime.now()))
        else:
            print('Chan file already in path : ', chan_file_path)

    except Exception:
        traceback.print_exc()

