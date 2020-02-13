import os
import sys
from builtins import print
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from urllib.parse import urlparse, parse_qs

# from raincelldat.gen_raincell import create_raincell
# from inflowdat.get_inflow import create_inflow
# from outflowdat.gen_outflow import create_outflow
# from chandat.gen_chan import create_chan

from run_model import execute_flo2d, flo2d_model_completed

from extract.extract_water_level_hourly_run import upload_waterlevels
from extract.extract_discharge_hourly_run import upload_discharges

import subprocess

from os.path import join as pjoin
from datetime import datetime, timedelta

HOST_ADDRESS = '192.168.1.41'
HOST_PORT = 8088

WIN_OUTPUT_DIR_PATH = r"D:\flo2d_output"
WIN_HOME_DIR_PATH = r"D:\DSS-Framework\weather_models\flo2d"
WIN_FLO2D_DATA_MANAGER_PATH = r"D:\curw_flo2d_data_manager"


CREATE_CHAN_CMD = '.\input\chan\gen_chan.py -m "{}" -s "{}" -d "{}"'
CREATE_RAINCELL_CMD = '.\input\raincell\gen_raincell.py -m "{}" -s "{}" -e "{}" -d "{}" -M "{}"'
CREATE_INFLOW_250_CMD = '.\input\inflow\get_inflow_250.py -s "{}" -e "{}" -d "{}" -M "{}"'
CREATE_INFLOW_150_CMD = '.\input\inflow\get_inflow_150.py -s "{}" -e "{}" -d "{}" -M "{}"'
CREATE_OUTFLOW_CMD = '.\input\outflow\gen_outflow.py -m "{}" -s "{}" -e "{}" -d "{}" -M "{}"'
EXTRACT_OUTPUT_CMD = ''


def set_daily_dir(run_date, run_time):
    start_datetime = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    run_time = start_datetime.strftime('%H-%M-%S')
    dir_path = pjoin(WIN_OUTPUT_DIR_PATH, 'output', run_date, run_time)
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
        except OSError as e:
            print(str(e))
    print('set_daily_dir|dir_path : ', dir_path)
    return dir_path


def run_input_file_generation_methods(cmd):
    try:
        print('run_input_file_generation_methods|cmd: ', cmd)
        os.chdir(WIN_FLO2D_DATA_MANAGER_PATH)
        print('run_input_file_generation_methods|getcwd : ', os.getcwd())
        subprocess.call(cmd, shell=True)
        return {'response': 'success'}
    except Exception as ex:
        print('run_input_file_generation_methods|Exception: ', str(ex))
        return {'response': 'fail'}


def get_input_file_creation_params(query_components, input_file_type=None):
    try:
        print('get_input_file_creation_params|query_components : ', query_components)

        if query_components["run_date"]:
            [run_date] = query_components["run_date"]
        else:
            run_date = datetime.now().strftime('%Y-%m-%d')

        if query_components["run_time"]:
            [run_time] = query_components["run_time"]
        else:
            run_time = datetime.now().strftime('%H:00:00')

        if query_components["model"]:  # "flo2d_250"/"flo2d_150"
            [model] = query_components["model"]
        else:
            model = 'flo2d_250'

        if query_components["forward"]:
            [forward] = query_components["forward"]
        else:
            if model == 'flo2d_250':
                forward = 3
            else:
                forward = 5

        if query_components["backward"]:
            [backward] = query_components["backward"]
        else:
            if model == 'flo2d_250':
                backward = 2
            else:
                backward = 3
        ts_start_end = get_ts_start_end(run_date, run_time, int(forward), int(backward))
        if input_file_type == 'inflow' or input_file_type == 'outflow' or input_file_type == 'raincell':
            [pop_method] = query_components["pop_method"]
            return {'run_date': run_date, 'run_time': run_time, 'ts_start': ts_start_end['ts_start'],
                    'ts_end': ts_start_end['ts_end'], 'model': model, 'pop_method': pop_method}
        else:
            return {'run_date': run_date, 'run_time': run_time, 'ts_start': ts_start_end['ts_start'],
                    'ts_end': ts_start_end['ts_end'], 'model': model}
    except Exception as e:
        print('get_input_file_creation_params|Exception : ', str(e))


def get_ts_start_end(run_date, run_time, forward=3, backward=2):
    result = {}
    run_datetime = datetime.strptime('%s %s' % (run_date, '00:00:00'), '%Y-%m-%d %H:%M:%S')
    ts_start_datetime = run_datetime - timedelta(days=backward)
    ts_end_datetime = run_datetime + timedelta(days=forward)
    run_datetime = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    result['ts_start'] = ts_start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    result['run_time'] = run_datetime.strftime('%Y-%m-%d %H:%M:%S')
    result['ts_end'] = ts_end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    print(result)
    return result


class StoreHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.timeout = 2100
        print('Handle GET request...')

        if self.path.startswith('/create-raincell'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('create-raincell')
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_file_creation_params(query_components, 'raincell')
                print('StoreHandler|create-raincell|params : ', params)

                dir_path = set_daily_dir(params['run_date'], params['run_time'])
                try:
                    # create_raincell(dir_path, params['run_date'], params['run_time'],
                    #                 params['forward'], params['backward'], params['model'])
                    #CREATE_RAINCELL_CMD = '.\input\raincell\gen_raincell.py -m {} -s "{}" -e "{}" -d "{}" -M "{}"'
                    command = CREATE_RAINCELL_CMD.format(params['model'], params['ts_start'],
                                                         params['ts_end'], dir_path, params['pop_method'])
                    print('create-raincell|command : ', command)
                    response = run_input_file_generation_methods(command)
                except Exception as e:
                    print(str(e))
                    response = {'response': 'fail'}
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-inflow'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('create-inflow')
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_file_creation_params(query_components, 'inflow')
                print('StoreHandler|create-inflow|params : ', params)
                dir_path = set_daily_dir(params['run_date'], params['run_time'])

                # create_inflow(dir_path, params['run_date'], params['run_time'],
                #               params['forward'], params['backward'], params['model'])
                if params['model'] == 'flo2d_250':
                    #CREATE_INFLOW_250_CMD = '.\input\inflow\get_inflow_250.py -s "{}" -e "{}" -d "{}" -M "{}"'
                    command = CREATE_INFLOW_250_CMD.format(params['ts_start'], params['ts_end'], dir_path,
                                                           params['pop_method'])
                else:
                    command = CREATE_INFLOW_150_CMD.format(params['ts_start'], params['ts_end'], dir_path,
                                                           params['pop_method'])
                print('create-inflow|command : ', command)
                response = run_input_file_generation_methods(command)
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-outflow'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('create-outflow')
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_file_creation_params(query_components, 'outflow')
                print('StoreHandler|create-outflow|params : ', params)

                dir_path = set_daily_dir(params['run_date'], params['run_time'])

                # create_outflow(dir_path, params['run_date'], params['run_time'],
                #                params['forward'], params['backward'], params['model'])
                #CREATE_OUTFLOW_CMD = '.\input\outflow\gen_outflow.py -m {} -s "{}" -d "{}" -M "{}"'
                command = CREATE_OUTFLOW_CMD.format(params['model'], params['ts_start'],
                                                    params['ts_end'], dir_path, params['pop_method'])
                print('create-outflow|command : ', command)
                response = run_input_file_generation_methods(command)
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-chan'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('create-chan')
            try:
                query_components = parse_qs(urlparse(self.path).query)

                params = get_input_file_creation_params(query_components, 'chan')
                print('StoreHandler|create-chan|params : ', params)
                dir_path = set_daily_dir(params['run_date'], params['run_time'])

                # create_chan(dir_path, params['run_date'], params['run_time'],
                #             params['forward'], params['backward'], params['model'])
                #CREATE_CHAN_CMD = '.\input\chan\gen_chan.py -m {} -s "{}" -d "{}"'
                command = CREATE_CHAN_CMD.format(params['model'], params['ts_start'], dir_path)
                print('create-chan|command : ', command)
                response = run_input_file_generation_methods(command)
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/run-flo2d'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('run-flo2d')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_file_creation_params(query_components, 'run')
                print('StoreHandler|run-flo2d|params : ', params)

                dir_path = set_daily_dir(params['run_date'], params['run_time'])

                # execute_flo2d(dir_path, params['run_date'], params['run_time'],
                #               params['forward'], params['backward'], params['model'])
                execute_flo2d(dir_path, params['run_date'], params['run_time'])
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/extract-data'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('extract-data')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_file_creation_params(query_components, 'extract')
                print('StoreHandler|extract-data|params : ', params)
                dir_path = set_daily_dir(params['run_date'], params['run_time'])

                # upload_waterlevels_curw(dir_path, ts_start_date, ts_start_time)
                upload_waterlevels(dir_path, params['run_date'], params['run_time'],
                                   params['forward'], params['backward'], params['model'])
                # upload discharges to curw_fcst database
                upload_discharges(dir_path, params['run_date'], params['run_time'],
                                  params['forward'], params['backward'], params['model'])
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))


if __name__ == '__main__':
    try:
        print('starting server...')
        arguments = len(sys.argv) - 1
        if arguments > 0:
            host_address = sys.argv[1]
            host_port = sys.argv[2]
        else:
            host_address = HOST_ADDRESS
            host_port = HOST_PORT
        server_address = (host_address, host_port)
        httpd = HTTPServer(server_address, StoreHandler)
        print('server running on host {} and port {} ...'.format(host_address, host_port))
        httpd.serve_forever()
    except Exception as e:
        print(str(e))
