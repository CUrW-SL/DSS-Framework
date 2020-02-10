import os
import sys
from builtins import print
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from urllib.parse import urlparse, parse_qs

from raincelldat.gen_raincell import create_raincell
from inflowdat.get_inflow import create_inflow
from outflowdat.gen_outflow import create_outflow
from chandat.gen_chan import create_chan

from run_model import execute_flo2d, flo2d_model_completed

from extract.extract_water_level_hourly_run import upload_waterlevels
from extract.extract_discharge_hourly_run import upload_discharges

from os.path import join as pjoin
from datetime import datetime

HOST_ADDRESS = '192.168.1.41'
HOST_PORT = 8088

WIN_OUTPUT_DIR_PATH = r"D:\flo2d_output"
WIN_HOME_DIR_PATH = r"D:\DSS-Framework\weather_models\flo2d"


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


def get_input_file_creation_params(query_components):
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
        return {'run_date': run_date, 'run_time': run_time, 'forward': int(forward),
                'backward': int(backward), 'model': model}
    except Exception as e:
        print('get_input_file_creation_params|Exception : ', str(e))


class StoreHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.timeout = 2100
        print('Handle GET request...')

        if self.path.startswith('/create-raincell'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('create-raincell')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_file_creation_params(query_components)
                print('StoreHandler|create-raincell|params : ', params)

                dir_path = set_daily_dir(params['run_date'], params['run_time'])
                try:
                    create_raincell(dir_path, params['run_date'], params['run_time'],
                                    params['forward'], params['backward'], params['model'])
                    response = {'response': 'success'}
                except Exception as e:
                    response = {'response': 'fail'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-inflow'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('create-inflow')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_file_creation_params(query_components)
                print('StoreHandler|create-inflow|params : ', params)
                dir_path = set_daily_dir(params['run_date'], params['run_time'])

                create_inflow(dir_path, params['run_date'], params['run_time'],
                              params['forward'], params['backward'], params['model'])
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-chan'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('create-chan')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)

                params = get_input_file_creation_params(query_components)
                print('StoreHandler|create-chan|params : ', params)
                dir_path = set_daily_dir(params['run_date'], params['run_time'])

                create_chan(dir_path, params['run_date'], params['run_time'],
                            params['forward'], params['backward'], params['model'])
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-outflow'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('create-outflow')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_file_creation_params(query_components)
                print('StoreHandler|create-outflow|params : ', params)

                dir_path = set_daily_dir(params['run_date'], params['run_time'])

                create_outflow(dir_path, params['run_date'], params['run_time'],
                               params['forward'], params['backward'], params['model'])
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
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

                params = get_input_file_creation_params(query_components)
                print('StoreHandler|run-flo2d|params : ', params)

                dir_path = set_daily_dir(params['run_date'], params['run_time'])

                # execute_flo2d(dir_path, params['run_date'], params['run_time'],
                #               params['forward'], params['backward'], params['model'])
                execute_flo2d(dir_path, params['run_date'], params['run_time'])
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
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

                params = get_input_file_creation_params(query_components)
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
