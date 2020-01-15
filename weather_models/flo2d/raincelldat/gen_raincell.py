import pymysql
import getopt
from datetime import datetime, timedelta
import traceback
import os
import sys

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def append_to_file(file_name, data):
    with open(file_name, 'a+') as f:
        f.write('\n'.join(data))


def check_time_format(time):
    try:
        time = datetime.strptime(time, DATE_TIME_FORMAT)
        if time.strftime('%S') != '00':
            print("Seconds should be always 00")
        if time.strftime('%M') not in ('05', '10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '00'):
            print("Minutes should be multiple of 5")
            exit(1)
            return True
    except Exception:
        traceback.print_exc()
        print("Time {} is not in proper format".format(time))
        exit(1)


def prepare_raincell(raincell_file_path, start_time, end_time,
                     target_model="flo2d_250", interpolation_method="MME"):
    """
    Create raincell for flo2d
    :param raincell_file_path:
    :param start_time: Raincell start time (e.g: "2019-06-05 00:00:00")
    :param end_time: Raincell start time (e.g: "2019-06-05 23:30:00")
    :param target_model: FLO2D model (e.g. flo2d_250, flo2d_150)
    :param interpolation_method: value interpolation method (e.g. "MME")
    :return:
    """
    connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=DB,
                                 cursorclass=pymysql.cursors.DictCursor)
    print("Connected to database")

    end_time = datetime.strptime(end_time, DATE_TIME_FORMAT)
    start_time = datetime.strptime(start_time, DATE_TIME_FORMAT)

    if end_time < start_time:
        print("start_time should be less than end_time")
        exit(1)

    # find max end time
    try:
        with connection.cursor() as cursor0:
            cursor0.callproc('get_ts_end', (target_model, interpolation_method))
            max_end_time = cursor0.fetchone()['time']

    except Exception as e:
        traceback.print_exc()
        max_end_time = datetime.strptime((datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 23:30:00'),
                                         DATE_TIME_FORMAT)

    min_start_time = datetime.strptime("2019-06-28 00:00:00", DATE_TIME_FORMAT)

    if end_time > max_end_time:
        end_time = max_end_time

    if start_time < min_start_time:
        start_time = min_start_time

    if target_model == "flo2d_250":
        timestep = 5
    elif target_model == "flo2d_150":
        timestep = 15

    length = int(((end_time - start_time).total_seconds() / 60) / timestep)

    write_to_file(raincell_file_path,
                  ['{} {} {} {}\n'.format(timestep, length, start_time.strftime(DATE_TIME_FORMAT),
                                          end_time.strftime(DATE_TIME_FORMAT))])
    try:
        timestamp = start_time
        while timestamp < end_time:
            raincell = []
            timestamp = timestamp + timedelta(minutes=timestep)
            # Extract raincell from db
            with connection.cursor() as cursor1:
                cursor1.callproc('prepare_flo2d_raincell', (target_model, interpolation_method, timestamp))
                for result in cursor1:
                    raincell.append('{} {}'.format(result.get('cell_id'), '%.1f' % result.get('value')))
                raincell.append('')
            append_to_file(raincell_file_path, raincell)
            print(timestamp)
    except Exception as ex:
        traceback.print_exc()
    finally:
        connection.close()
        print("{} raincell generation process completed".format(datetime.now()))


# def get_ts_start_end(run_date, run_time, forward=3, backward=2):
#     result = []
#     """
#     method for geting timeseries start and end using input params.
#     :param run_date:run_date: string yyyy-mm-ddd
#     :param run_time:run_time: string hh:mm:ss
#     :param forward:int
#     :param backward:int
#     :return: tuple (string, string)
#     """
#     run_datetime = datetime.strptime('%s %s' % (run_date, '00:00:00'), '%Y-%m-%d %H:%M:%S')
#     ts_start_datetime = run_datetime - timedelta(days=backward)
#     ts_end_datetime = run_datetime + timedelta(days=forward)
#     result.append(ts_start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
#     result.append(ts_end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
#     print(result)
#     return result


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
    Usage: ./gen_raincell.py [-m flo2d_XXX][-s "YYYY-MM-DD HH:MM:SS"] [-e "YYYY-MM-DD HH:MM:SS"]
    
    -h  --help          Show usage
    -m  --model         FLO2D model (e.g. flo2d_250, flo2d_150). Default is flo2d_250.
    -s  --start_time    Raincell start time (e.g: "2019-06-05 00:00:00"). Default is 23:30:00, 3 days before today.
    -e  --end_time      Raincell end time (e.g: "2019-06-05 23:30:00"). Default is 23:30:00, tomorrow.
    """
    print(usageText)


if __name__ == "__main__":

    try:
        start_time = None
        end_time = None
        flo2d_model = None

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

        os.chdir(r"D:\raincells")
        os.system(r"venv\Scripts\activate")

        if start_time is None:
            start_time = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d 23:30:00')
        else:
            check_time_format(start_time)

        if end_time is None:
            end_time = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 23:30:00')
        else:
            check_time_format(end_time)

        if flo2d_model is None:
            flo2d_model = "flo2d_250"
        elif flo2d_model not in ("flo2d_250", "flo2d_150"):
            print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\"")
            exit(1)

        raincell_file_path = os.path.join(r"D:\raincells",
                                          'RAINCELL_{}_{}_{}.DAT'.format(flo2d_model, start_time, end_time).replace(' ',
                                                                                                                    '_').replace(
                                              ':', '-'))

        if not os.path.isfile(raincell_file_path):
            print("{} start preparing raincell".format(datetime.now()))
            prepare_raincell(raincell_file_path,
                             target_model=flo2d_model, start_time=start_time, end_time=end_time)
            # print(raincell_file_path, flo2d_model, start_time, end_time)
            print("{} completed preparing raincell".format(datetime.now()))
        else:
            print('Raincell file already in path : ', raincell_file_path)

        # os.system(r"deactivate")

    except Exception:
        traceback.print_exc()
