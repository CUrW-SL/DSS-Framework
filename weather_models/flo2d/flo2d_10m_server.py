import os
import sys
from builtins import print
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from urllib.parse import urlparse, parse_qs
from run_model import execute_flo2d, flo2d_model_completed
from gen_old_outflow import create_outflow_old
from create_raincell import create_event_raincell
from multi_ascii import generate_ascii_set
from single_ascii import generate_flood_map
from file_upload import upload_file_to_bucket
import subprocess
from os.path import join as pjoin
from datetime import datetime, timedelta
import threading

HOST_ADDRESS = '10.138.0.18'
HOST_PORT = 8088

WIN_OUTPUT_DIR_PATH = r"D:\flo2d_output"
WIN_HOME_DIR_PATH = r"D:\DSS-Framework\weather_models\flo2d"
WIN_FLO2D_DATA_MANAGER_PATH = r"D:\curw_flo2d_data_manager"
GOOGLE_BUCKET_KEY_PATH = r"D:\DSS-Framework\weather_models\flo2d\uwcc-admin.json"
BUCKET_NAME = 'curwsl_nfs'

CREATE_CHAN_CMD = '.\gen_chan.py -m "{}" -s "{}" -d "{}"'
CREATE_CHAN_LOCAL_CMD = '.\gen_chan.py -m "{}" -s "{}" -d "{}" -E'

CREATE_RAINCELL_CMD = '.\gen_raincell.py -m "{}" -s "{}" -e "{}" -d "{}" -M "{}"'
CREATE_RAINCELL_LOCAL_CMD = '.\gen_raincell.py -m "{}" -s "{}" -e "{}" -d "{}" -M "{}" -E'

CREATE_INFLOW_250_CMD = '.\gen_250_inflow.py -s "{}" -e "{}" -d "{}" -M "{}"'
CREATE_INFLOW_250_LOCAL_CMD = '.\gen_250_inflow.py -s "{}" -e "{}" -d "{}" -M "{}" -E'

CREATE_INFLOW_150_CMD = '.\gen_150_inflow.py -s "{}" -e "{}" -d "{}" -M "{}"'
CREATE_INFLOW_150_LOCAL_CMD = '.\gen_150_inflow.py -s "{}" -e "{}" -d "{}" -M "{}" -E'

CREATE_OUTFLOW_CMD = '.\gen_outflow.py -m "{}" -s "{}" -e "{}" -d "{}" -M "{}"'
CREATE_OUTFLOW_LOCAL_CMD = '.\gen_outflow.py -m "{}" -s "{}" -e "{}" -d "{}" -M "{}" -E'

EXTRACT_WATER_LEVEL_CMD = '.\extract_water_level.py -m "{}" -s "{}" -r "{}" -d "{}" -t "{}"'
EXTRACT_WATER_LEVEL_LOCAL_CMD = '.\extract_water_level.py -m "{}" -s "{}" -r "{}" -d "{}" -t "{}" -E '

EXTRACT_WATER_DISCHARGE_CMD = '.\extract_discharge.py -m "{}" -s "{}" -r "{}" -d "{}" -t "{}"'
EXTRACT_WATER_DISCHARGE_LOCAL_CMD = '.\extract_discharge.py -m "{}" -s "{}" -r "{}" -d "{}" -t "{}" -E '


def set_daily_dir(target_model, run_date, run_time):
    start_datetime = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    run_time = start_datetime.strftime('%H-%M-%S')
    dir_path = pjoin(WIN_OUTPUT_DIR_PATH, target_model, run_date, run_time)
    print('set_daily_dir|dir_path : ', dir_path)
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
        except OSError as e:
            print(str(e))
    print('set_daily_dir|dir_path : ', dir_path)
    return dir_path


def run_input_file_generation_methods(dir_path, command):
    try:
        print('run_input_file_generation_methods|dir_path: ', dir_path)
        print('run_input_file_generation_methods|command: ', command)
        os.chdir(dir_path)
        print('run_input_file_generation_methods|getcwd : ', os.getcwd())
        subprocess.call(command, shell=True)
        return {'response': 'success'}
    except Exception as ex:
        print('run_input_file_generation_methods|Exception: ', str(ex))
        return {'response': 'fail'}


def get_input_params(query_components, input_type=None):
    try:
        print('get_input_params|query_components : ', query_components)

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

        if query_components["run_type"]:  # "production"/"event"
            [run_type] = query_components["run_type"]
        else:
            run_type = 'production'

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
        if input_type == 'inflow' or input_type == 'outflow' or input_type == 'raincell':
            [pop_method] = query_components["pop_method"]
            return {'run_date': run_date, 'run_time': run_time, 'ts_start': ts_start_end['ts_start'],
                    'ts_end': ts_start_end['ts_end'], 'run_date_time': ts_start_end['run_time'], 'model': model,
                    'pop_method': pop_method, 'run_type': run_type}
        elif input_type == 'extract':
            [sim_tag] = query_components["sim_tag"]
            return {'run_date': run_date, 'run_time': run_time, 'ts_start': ts_start_end['ts_start'],
                    'ts_end': ts_start_end['ts_end'], 'run_date_time': ts_start_end['run_time'], 'model': model,
                    'sim_tag': sim_tag, 'run_type': run_type}
        else:
            return {'run_date': run_date, 'run_time': run_time, 'ts_start': ts_start_end['ts_start'],
                    'ts_end': ts_start_end['ts_end'], 'run_date_time': ts_start_end['run_time'],
                    'model': model, 'run_type': run_type}
    except Exception as e:
        print('get_input_params|Exception : ', str(e))


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

                params = get_input_params(query_components, 'raincell')
                print('StoreHandler|create-raincell|params : ', params)

                dir_path = set_daily_dir(params['model'], params['run_date'], params['run_time'])
                try:
                    command_dir_path = os.path.join(WIN_FLO2D_DATA_MANAGER_PATH, 'input', 'raincell')
                    if params['run_type'] == 'production':
                        command = CREATE_RAINCELL_CMD.format(params['model'], params['ts_start'],
                                                             params['ts_end'], dir_path, params['pop_method'])
                    else:
                        command = CREATE_RAINCELL_LOCAL_CMD.format(params['model'], params['ts_start'],
                                                                   params['ts_end'], dir_path, params['pop_method'])
                    print('create-raincell|command : ', command)
                    if params['pop_method'].isupper():
                        print('create-raincell|command_dir_path : ', command_dir_path)
                        response = run_input_file_generation_methods(command_dir_path, command)
                    else:
                        rule_name = params['pop_method']
                        print('create-raincell|rule_name : ', rule_name)
                        sim_tag = rule_name[:len(rule_name) - 3]
                        print('create-raincell|sim_tag : ', sim_tag)
                        source_id = int(rule_name[len(rule_name) - 2:])
                        print('create-raincell|source_id : ', source_id)
                        [forward] = query_components['forward']
                        [backward] = query_components['backward']
                        create_event_raincell(dir_path, params['run_date'], params['run_time'], int(forward),
                                              int(backward), 'flo2d_250', sim_tag, source_id)
                        response = {'response': 'success'}
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

                params = get_input_params(query_components, 'inflow')
                print('StoreHandler|create-inflow|params : ', params)
                dir_path = set_daily_dir(params['model'], params['run_date'], params['run_time'])

                command_dir_path = os.path.join(WIN_FLO2D_DATA_MANAGER_PATH, 'input', 'inflow')
                if params['model'] == 'flo2d_250':
                    if params['run_type'] == 'production':
                        command = CREATE_INFLOW_250_CMD.format(params['ts_start'], params['ts_end'], dir_path,
                                                           params['pop_method'])
                    else:
                        command = CREATE_INFLOW_250_LOCAL_CMD.format(params['ts_start'], params['ts_end'], dir_path,
                                                     params['pop_method'])
                else:
                    if params['run_type'] == 'production':
                        command = CREATE_INFLOW_150_CMD.format(params['ts_start'], params['ts_end'], dir_path,
                                                           params['pop_method'])
                    else:
                        command = CREATE_INFLOW_150_LOCAL_CMD.format(params['ts_start'], params['ts_end'], dir_path,
                                                               params['pop_method'])
                print('create-inflow|command : ', command)
                print('create-inflow|command_dir_path : ', command_dir_path)
                response = run_input_file_generation_methods(command_dir_path, command)
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

                params = get_input_params(query_components, 'outflow')
                print('StoreHandler|create-outflow|params : ', params)

                dir_path = set_daily_dir(params['model'], params['run_date'], params['run_time'])

                command_dir_path = os.path.join(WIN_FLO2D_DATA_MANAGER_PATH, 'input', 'outflow')
                if params['pop_method'] == 'CODE':
                    print('Generating outflow from old method.')
                    print('input parameters|[dir_path, ts_start, ts_end] : ', [dir_path, params['ts_start'],
                                                                               params['ts_end']])
                    create_outflow_old(dir_path, params['ts_start'], params['ts_end'])
                    response = {'response': 'success'}
                else:
                    if params['run_type'] == 'production':
                        command = CREATE_OUTFLOW_CMD.format(params['model'], params['ts_start'],
                                                        params['ts_end'], dir_path, params['pop_method'])
                    else:
                        command = CREATE_OUTFLOW_LOCAL_CMD.format(params['model'], params['ts_start'],
                                                            params['ts_end'], dir_path, params['pop_method'])
                    print('create-outflow|command : ', command)
                    print('create-outflow|command_dir_path : ', command_dir_path)
                    response = run_input_file_generation_methods(command_dir_path, command)
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

                params = get_input_params(query_components, 'chan')
                print('StoreHandler|create-chan|params : ', params)
                dir_path = set_daily_dir(params['model'], params['run_date'], params['run_time'])

                command_dir_path = os.path.join(WIN_FLO2D_DATA_MANAGER_PATH, 'input', 'chan')
                if params['run_type'] == 'production':
                    command = CREATE_CHAN_CMD.format(params['model'], params['ts_start'], dir_path)
                else:
                    command = CREATE_CHAN_LOCAL_CMD.format(params['model'], params['ts_start'], dir_path)
                print('create-chan|command : ', command)
                print('create-chan|command_dir_path : ', command_dir_path)
                response = run_input_file_generation_methods(command_dir_path, command)
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
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_params(query_components, 'run')
                print('StoreHandler|run-flo2d|params : ', params)

                dir_path = set_daily_dir(params['model'], params['run_date'], params['run_time'])

                execute_flo2d(dir_path, params['model'], params['run_date'], params['run_time'])
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/extract-water-level'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('extract-water-level')
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_params(query_components, 'extract')
                print('StoreHandler|extract-water-level|params : ', params)
                dir_path = set_daily_dir(params['model'], params['run_date'], params['run_time'])

                command_dir_path = os.path.join(WIN_FLO2D_DATA_MANAGER_PATH, 'output')
                params['run_type'] = 'event'
                print('StoreHandler|extract-water-level|params[run_type] : ', params['run_type'])
                if params['run_type'] == 'production':
                    command = EXTRACT_WATER_LEVEL_CMD.format(params['model'], params['ts_start'],
                                                         params['run_date_time'], dir_path, params['sim_tag'])
                else:
                    command = EXTRACT_WATER_LEVEL_LOCAL_CMD.format(params['model'], params['ts_start'],
                                                             params['run_date_time'], dir_path, params['sim_tag'])
                print('extract-water-level|command : ', command)
                print('extract-water-level|command_dir_path : ', command_dir_path)

                response = run_input_file_generation_methods(command_dir_path, command)
                print('extract-water-level|response : ', response)
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/extract-discharge'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('extract-discharge')
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_params(query_components, 'extract')
                print('StoreHandler|extract-discharge|params : ', params)
                dir_path = set_daily_dir(params['model'], params['run_date'], params['run_time'])

                command_dir_path = os.path.join(WIN_FLO2D_DATA_MANAGER_PATH, 'output')
                params['run_type'] = 'event'
                if params['run_type'] == 'production':
                    command = EXTRACT_WATER_DISCHARGE_CMD.format(params['model'], params['ts_start'],
                                                             params['run_date_time'], dir_path, params['sim_tag'])
                else:
                    command = EXTRACT_WATER_DISCHARGE_LOCAL_CMD.format(params['model'], params['ts_start'],
                                                                 params['run_date_time'], dir_path, params['sim_tag'])
                print('extract-discharge|command : ', command)
                print('extract-discharge|command_dir_path : ', command_dir_path)

                response = run_input_file_generation_methods(command_dir_path, command)
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-ascii'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('create-ascii')
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_params(query_components, 'extract')
                print('StoreHandler|create-ascii|params : ', params)
                dir_path = set_daily_dir(params['model'], params['run_date'], params['run_time'])

                ts_start_date_time = datetime.strptime(params['ts_start'], '%Y-%m-%d %H:%M:%S')
                ts_start_date = ts_start_date_time.strftime('%Y-%m-%d')
                print('create-ascii|generate_ascii_set|[ts_start_date, dir_path, params[\'model\']] :',
                      [ts_start_date, dir_path, params['model']])
                if generate_ascii_set(ts_start_date, dir_path, params['model']):
                    print('ascii files have generated.')
                    if params['model'] == 'flo2d_250':
                        flo2d_model = '250'
                    elif params['model'] == 'flo2d_150':
                        flo2d_model = '150'
                    elif params['model'] == 'flo2d_30':
                        flo2d_model = '30'
                    elif params['model'] == 'flo2d_10':
                        flo2d_model = '10'
                    bucket_file = 'flo2d/{}/{}/{}/multi_ascii.zip'.format(flo2d_model, params['run_date'],
                                                                          params['run_time'])
                    zip_file = os.path.join(dir_path, 'multi_ascii.zip')
                    print('StoreHandler|create-ascii|[BUCKET_NAME, zip_file, bucket_file] : ', [BUCKET_NAME, zip_file,
                                                                                                bucket_file])
                    upload_file_to_bucket(GOOGLE_BUCKET_KEY_PATH, BUCKET_NAME, zip_file, bucket_file)
                    response = {'response': 'success'}
                else:
                    response = {'response': 'fail'}
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-max-wl-map'):
            os.chdir(WIN_HOME_DIR_PATH)
            print('create-max-wl-map')
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)

                params = get_input_params(query_components, 'extract')
                print('StoreHandler|create-max-wl-map|params : ', params)
                dir_path = set_daily_dir(params['model'], params['run_date'], params['run_time'])

                ts_start_date_time = datetime.strptime(params['ts_start'], '%Y-%m-%d %H:%M:%S')
                ts_start_date = ts_start_date_time.strftime('%Y-%m-%d')
                print('create-max-wl-map|generate_ascii_set|[ts_start_date, dir_path, params[\'model\']] :',
                      [ts_start_date, dir_path, params['model']])
                if generate_flood_map(ts_start_date, dir_path, params['model']):
                    print('max water level map has generated.')
                    if params['model'] == 'flo2d_250':
                        flo2d_model = '250'
                    elif params['model'] == 'flo2d_150':
                        flo2d_model = '150'
                    elif params['model'] == 'flo2d_30':
                        flo2d_model = '30'
                    elif params['model'] == 'flo2d_10':
                        flo2d_model = '10'
                    bucket_file = 'flo2d/{}/{}/{}/max_wl_map.asc'.format(flo2d_model, params['run_date'],
                                                                         params['run_time'])
                    ascii_file = os.path.join(dir_path, 'max_wl_map.asc')
                    print('StoreHandler|create-max-wl-map|[BUCKET_NAME, ascii_file, bucket_file] : ',
                          [BUCKET_NAME, ascii_file, bucket_file])
                    upload_file_to_bucket(GOOGLE_BUCKET_KEY_PATH, BUCKET_NAME, ascii_file, bucket_file)
                    response = {'response': 'success'}
                else:
                    response = {'response': 'fail'}
            except Exception as e:
                print(str(e))
                response = {'response': 'fail'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/test-server-status'):
            print('----------------------------------------------')
            print('StoreHandler|test-server-status|self.server.server_address:', self.server.server_address)
            print('----------------------------------------------')
            reply = json.dumps({'response': 'server-running'})
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))


def boot_up_flo2d_server(server_name, host_address, host_port):
    print('boot_up_flo2d_server|[server_name, host_address, host_port] :', [server_name, host_address, host_port])
    try:
        daemon = threading.Thread(name=server_name,
                                  target=start_flo2d_server,
                                  args=(host_address, host_port))
        daemon.setDaemon(True)  # Set as a daemon so it will be killed once the main thread is dead.
        daemon.start()
        print('boot_up_flo2d_server|success on [server_name, host_address, host_port] :',
              [server_name, host_address, host_port])
    except Exception as ex:
        print('boot_up_flo2d_server|Exception : ', str(ex))
        return None


def start_flo2d_server(host_address, host_port):
    print('start_flo2d_server|[host_address, host_port] :', [host_address, host_port])
    try:
        server_address = (host_address, host_port)
        httpd = HTTPServer(server_address, StoreHandler)
        print('server running on host {} and port {} ...'.format(host_address, host_port))
        httpd.serve_forever()
        print('server has started on host {} and port {} ...'.format(host_address, host_port))
        print('start_flo2d_server|httpd :', httpd)
        return httpd
    except Exception as ex:
        print('start_flo2d_server|Exception : ', str(ex))
        return None


def stop_flo2d_server(httpd):
    print('start_flo2d_server|httpd :', httpd)
    try:
        httpd.shutdown()
        print('stop_flo2d_server|httpd :', httpd)
        return httpd
    except Exception as e:
        print('start_flo2d_server|Exception : ', str(e))
        return None

