#!"D:\curw_flo2d_data_manager\venv\Scripts\python.exe"
import pymysql
from datetime import datetime, timedelta
import traceback
import json
import os
import sys
import getopt

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DB_CONFIG_DIRECTORY = 'D:\curw_flo2d_data_manager'
ROOT_DIRECTORY = r"D:\flo2d_hourly"

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
# from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_HOST, CURW_SIM_PASSWORD, CURW_SIM_PORT, CURW_SIM_USERNAME
from db_adapter.curw_sim.timeseries import get_curw_sim_tidal_id
from db_adapter.curw_sim.timeseries.tide import Timeseries as TideTS


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


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def append_to_file(file_name, data):
    with open(file_name, 'a+') as f:
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


def prepare_tide_data_set(curw_sim_pool, method, grid_id, model, start, end):
    try:
        TS = TideTS(pool=curw_sim_pool)

        tide_id = get_curw_sim_tidal_id(pool=curw_sim_pool, method=method, grid_id=grid_id, model="flo2d_250")

        tide_ts = TS.get_timeseries(id_=tide_id, start_date=start, end_date=end)
        tide_data = []

        for i in range(len(tide_ts)):
            time_col = (str('%.3f' % (((tide_ts[i][0] - tide_ts[0][0]).total_seconds()) / 3600))).rjust(16)
            value_col = (str('%.3f' % (tide_ts[i][1]))).rjust(16)
            tide_data.append('S' + time_col + value_col)
        return tide_data

    except Exception as e:
        print(traceback.print_exc())


def prepare_outflow_250(outflow_file_path, start, end, tide_config_dict, method, model):
    try:
        curw_sim_pool = get_Pool(host=con_params.CURW_SIM_HOST, user=con_params.CURW_SIM_USERNAME,
                                 password=con_params.CURW_SIM_PASSWORD, port=con_params.CURW_SIM_PORT,
                                 db=con_params.CURW_SIM_DATABASE)

        outflow = []

        outflow.append('K              91')
        outflow.append('K             171')
        outflow.append('K             214')
        outflow.append('K             491')

        outflow.append('N             134               1')
        tide_data = prepare_tide_data_set(curw_sim_pool=curw_sim_pool, method=method,
                                          grid_id=tide_config_dict.get('134'),
                                          model=model, start=start, end=end)
        outflow.extend(tide_data)

        outflow.append('N             220               1')
        tide_data = prepare_tide_data_set(curw_sim_pool=curw_sim_pool, method=method,
                                          grid_id=tide_config_dict.get('220'),
                                          model=model, start=start, end=end)
        outflow.extend(tide_data)

        outflow.append('N             261               1')
        tide_data = prepare_tide_data_set(curw_sim_pool=curw_sim_pool, method=method,
                                          grid_id=tide_config_dict.get('261'),
                                          model=model, start=start, end=end)
        outflow.extend(tide_data)

        outflow.append('N             558               1')
        tide_data = prepare_tide_data_set(curw_sim_pool=curw_sim_pool, method=method,
                                          grid_id=tide_config_dict.get('558'),
                                          model=model, start=start, end=end)
        outflow.extend(tide_data)

        write_to_file(outflow_file_path, data=outflow)

        tail_file = open(os.path.join(ROOT_DIRECTORY, "outflow", "tail_250.txt"), "r")
        tail = tail_file.read()
        tail_file.close()

        append_file_to_file(outflow_file_path, file_content=tail)

    except Exception as e:
        print(traceback.print_exc())
    finally:
        destroy_Pool(curw_sim_pool)
        print("Outflow generated")


def prepare_outflow_150(outflow_file_path, start, end, tide_config_dict, method, model):
    try:
        curw_sim_pool = get_Pool(host=con_params.CURW_SIM_HOST, user=con_params.CURW_SIM_USERNAME,
                                 password=con_params.CURW_SIM_PASSWORD, port=con_params.CURW_SIM_PORT,
                                 db=con_params.CURW_SIM_DATABASE)

        outflow = []

        outflow.append('K             290')
        outflow.append('K             416')
        outflow.append('K             488')
        outflow.append('K            1218')

        outflow.append('N             356               1')
        tide_data = prepare_tide_data_set(curw_sim_pool=curw_sim_pool, method=method,
                                          grid_id=tide_config_dict.get('356'),
                                          model=model, start=start, end=end)
        outflow.extend(tide_data)

        outflow.append('N             497               1')
        tide_data = prepare_tide_data_set(curw_sim_pool=curw_sim_pool, method=method,
                                          grid_id=tide_config_dict.get('497'),
                                          model=model, start=start, end=end)
        outflow.extend(tide_data)

        outflow.append('N             568               1')
        tide_data = prepare_tide_data_set(curw_sim_pool=curw_sim_pool, method=method,
                                          grid_id=tide_config_dict.get('568'),
                                          model=model, start=start, end=end)
        outflow.extend(tide_data)

        outflow.append('N            1330               1')
        tide_data = prepare_tide_data_set(curw_sim_pool=curw_sim_pool, method=method,
                                          grid_id=tide_config_dict.get('1330'),
                                          model=model, start=start, end=end)
        outflow.extend(tide_data)

        write_to_file(outflow_file_path, data=outflow)

        tail_file = open(os.path.join(ROOT_DIRECTORY, "outflow", "tail_150.txt"), "r")
        tail = tail_file.read()
        tail_file.close()

        append_file_to_file(outflow_file_path, file_content=tail)

    except Exception as e:
        print(traceback.print_exc())
    finally:
        destroy_Pool(curw_sim_pool)
        print("Outflow generated")


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
    Prepare outflow for Flo2D 250 & Flo2D 150
    ------------------------------------------
    Usage: .\input\outflow\gen_outflow.py [-m flo2d_XXX] [-s "YYYY-MM-DD HH:MM:SS"] [-e "YYYY-MM-DD HH:MM:SS"] [-d "directory_path"] 
    [-M XXX] [-c XXXXXXXXX]

    -h  --help          Show usage
    -m  --model         FLO2D model (e.g. flo2d_250, flo2d_150). Default is flo2d_250.
    -s  --start_time    Outflow start time (e.g: "2019-06-05 00:00:00"). Default is 00:00:00, 2 days before today.
    -e  --end_time      Outflow end time (e.g: "2019-06-05 23:00:00"). Default is 00:00:00, tomorrow.
    -d  --dir           Outflow file generation location (e.g: "C:\\udp_150\\2019-09-23")
    -M  --method        Outflow calculation method (e.g: "MME", "TSF")
    -c  --tide_config   Tidal id - grid configuration name (e.g: "tide_ids_150"). Default is "tide_ids_[flo2d version]".
    """
    print(usageText)


def create_outflow(output_dir, start_time, end_time, flo2d_model, method):
    set_db_config_file_path(os.path.join(DB_CONFIG_DIRECTORY, 'db_adapter_config.json'))

    """ formats to be changed as follows
    {
      "tide_ids_150_v1": {
                    "356": "tide_colombo",
                    "497": "tide_colombo",
                    "568": "tide_colombo",
                    "1330": "tide_colombo"
                }
    }

    {
      "tide_ids_150": {
                    "356": "tide_colombo",
                    "497": "tide_colombo",
                    "568": "tide_colombo",
                    "1330": "tide_mattakkuliya"
                }
    }

    """

    try:

        # start_time = None
        # end_time = None
        # flo2d_model = None
        # for both 150 and 250 models, curw_sim timeseies populated for 250 is used
        # method = "TSF"
        # output_dir = None
        file_name = 'OUTFLOW.DAT'
        tide_config = None

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:s:e:d:M:c:",
                                       ["help", "flo2d_model=", "start_time=", "end_time=", "dir=", "method=",
                                        "tide_config="])
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
            elif opt in ("-d", "--dir"):
                output_dir = arg.strip()
            elif opt in ("-M", "--method"):
                method = arg.strip()
            elif opt in ("-c", "--tide_config"):
                tide_config = arg.strip()

        if flo2d_model is None:
            flo2d_model = "flo2d_250"
        elif flo2d_model not in ("flo2d_250", "flo2d_150"):
            print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\"")
            exit(1)

        if start_time is None:
            start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=start_time)

        if end_time is None:
            end_time = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=end_time)

        if output_dir is not None:
            outflow_file_path = os.path.join(output_dir, file_name)
        else:
            outflow_file_path = os.path.join(r"D:\outflow",
                                             'OUTFLOW_{}_{}_{}.DAT'.format(flo2d_model, start_time, end_time).replace(
                                                 ' ', '_').replace(':', '-'))

        makedir_if_not_exist_given_filepath(outflow_file_path)

        flo2d_version = flo2d_model.split('_')[1]

        # Load config details and db connection params
        config = json.loads(open(os.path.join(ROOT_DIRECTORY, "outflow", "config_{}.json"
                                              .format(flo2d_version))).read())

        if tide_config is None:
            tide_config = "tide_ids_{}".format(flo2d_version)

        tide_config_dict = config[tide_config]

        if not os.path.isfile(outflow_file_path):
            print("{} start preparing outflow".format(datetime.now()))
            if flo2d_model == "flo2d_250":
                prepare_outflow_250(outflow_file_path, start=start_time, end=end_time,
                                    tide_config_dict=tide_config_dict,
                                    method=method, model=flo2d_model)
            elif flo2d_model == "flo2d_150":
                prepare_outflow_150(outflow_file_path, start=start_time, end=end_time,
                                    tide_config_dict=tide_config_dict,
                                    method=method, model=flo2d_model)
            metadata = {
                "outflow": {
                    "tag": method,
                    "model": flo2d_model,
                    "tide_ids": json.dumps(tide_config)
                }
            }
            save_metadata_to_file(input_filepath=outflow_file_path, metadata=metadata)
            print("{} completed preparing outflow".format(datetime.now()))
        else:
            print('Outflow file already in path : ', outflow_file_path)

    except Exception:
        traceback.print_exc()


if __name__ == "__main__":

    set_db_config_file_path(os.path.join(DB_CONFIG_DIRECTORY, 'db_adapter_config.json'))

    """ formats to be changed as follows
    {
      "tide_ids_150_v1": {
                    "356": "tide_colombo",
                    "497": "tide_colombo",
                    "568": "tide_colombo",
                    "1330": "tide_colombo"
                }
    }

    {
      "tide_ids_150": {
                    "356": "tide_colombo",
                    "497": "tide_colombo",
                    "568": "tide_colombo",
                    "1330": "tide_mattakkuliya"
                }
    }

    """

    try:

        start_time = None
        end_time = None
        flo2d_model = None
        # for both 150 and 250 models, curw_sim timeseies populated for 250 is used
        method = "TSF"
        output_dir = None
        file_name = 'OUTFLOW.DAT'
        tide_config = None

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:s:e:d:M:c:",
                                       ["help", "flo2d_model=", "start_time=", "end_time=", "dir=", "method=",
                                        "tide_config="])
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
            elif opt in ("-d", "--dir"):
                output_dir = arg.strip()
            elif opt in ("-M", "--method"):
                method = arg.strip()
            elif opt in ("-c", "--tide_config"):
                tide_config = arg.strip()

        if flo2d_model is None:
            flo2d_model = "flo2d_250"
        elif flo2d_model not in ("flo2d_250", "flo2d_150"):
            print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\"")
            exit(1)

        if start_time is None:
            start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=start_time)

        if end_time is None:
            end_time = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=end_time)

        if output_dir is not None:
            outflow_file_path = os.path.join(output_dir, file_name)
        else:
            outflow_file_path = os.path.join(r"D:\outflow",
                                             'OUTFLOW_{}_{}_{}.DAT'.format(flo2d_model, start_time, end_time).replace(
                                                 ' ', '_').replace(':', '-'))

        makedir_if_not_exist_given_filepath(outflow_file_path)

        flo2d_version = flo2d_model.split('_')[1]

        # Load config details and db connection params
        config = json.loads(open(os.path.join(ROOT_DIRECTORY, "outflow", "config_{}.json"
                                              .format(flo2d_version))).read())

        if tide_config is None:
            tide_config = "tide_ids_{}".format(flo2d_version)

        tide_config_dict = config[tide_config]

        if not os.path.isfile(outflow_file_path):
            print("{} start preparing outflow".format(datetime.now()))
            if flo2d_model == "flo2d_250":
                prepare_outflow_250(outflow_file_path, start=start_time, end=end_time,
                                    tide_config_dict=tide_config_dict,
                                    method=method, model=flo2d_model)
            elif flo2d_model == "flo2d_150":
                prepare_outflow_150(outflow_file_path, start=start_time, end=end_time,
                                    tide_config_dict=tide_config_dict,
                                    method=method, model=flo2d_model)
            metadata = {
                "outflow": {
                    "tag": method,
                    "model": flo2d_model,
                    "tide_ids": json.dumps(tide_config)
                }
            }
            save_metadata_to_file(input_filepath=outflow_file_path, metadata=metadata)
            print("{} completed preparing outflow".format(datetime.now()))
        else:
            print('Outflow file already in path : ', outflow_file_path)

    except Exception:
        traceback.print_exc()

