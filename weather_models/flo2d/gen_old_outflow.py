import json
import os
import sys
from datetime import datetime, timedelta
from curwmysqladapter import MySQLAdapter

CSV_NUM_METADATA_LINES = 2
DAT_WIDTH = 12
TIDAL_FORECAST_ID = "ebcc2df39aea35de15cca81bc5f15baffd94bcebf3f169add1fd43ee1611d367"


def get_forecast_timeseries(my_adapter, my_event_id, my_opts):
    existing_timeseries = my_adapter.retrieve_timeseries([my_event_id], my_opts)
    new_timeseries = []
    if len(existing_timeseries) > 0 and len(existing_timeseries[0]['timeseries']) > 0:
        existing_timeseries = existing_timeseries[0]['timeseries']
        for ex_step in existing_timeseries:
            if ex_step[0] - ex_step[0].replace(minute=0, second=0, microsecond=0) > timedelta(minutes=30):
                new_timeseries.append(
                    [ex_step[0].replace(minute=0, second=0, microsecond=0) + timedelta(hours=1), ex_step[1]])
            else:
                new_timeseries.append(
                    [ex_step[0].replace(minute=0, second=0, microsecond=0), ex_step[1]])

    return new_timeseries


def create_outflow_old(dir_path, ts_start, ts_end):
    outflow_file_path = os.path.join(dir_path, 'OUTFLOW.DAT')
    init_tidal_path = os.path.join(os.getcwd(), 'outflowdat', 'INITTIDAL.CONF')
    config_path = os.path.join(os.getcwd(), 'outflowdat', 'config_old.json')
    print('create_outflow_old|outflow_file_path : ', outflow_file_path)
    print('create_outflow_old|init_tidal_path : ', init_tidal_path)
    print('create_outflow_old|config_path : ', config_path)
    with open(config_path) as json_file:
        config = json.load(json_file)
        adapter = MySQLAdapter(host=config['db_host'], user=config['db_user'], password=config['db_password'],
                               db=config['db_name'])
        opts = {
            'from': ts_start,
            'to': ts_end,
        }
        tidal_timeseries = get_forecast_timeseries(adapter, TIDAL_FORECAST_ID, opts)
        if len(tidal_timeseries) > 0:
            print('tidal_timeseries::', len(tidal_timeseries), tidal_timeseries[0], tidal_timeseries[-1])
        else:
            print('No data found for tidal timeseries: ', tidal_timeseries)
            sys.exit(1)
        adapter.close()
        print('Open FLO2D OUTFLOW ::', outflow_file_path)
        outflow_file = open(outflow_file_path, 'w')
        lines = []

        print('Reading INIT TIDAL CONF...')
        with open(init_tidal_path) as initTidalConfFile:
            initTidalLevels = initTidalConfFile.readlines()
            for initTidalLevel in initTidalLevels:
                if len(initTidalLevel.split()):  # Check if not empty line
                    lines.append(initTidalLevel)
                    if initTidalLevel[0] == 'N':
                        lines.append('{0} {1:{w}} {2:{w}}\n'.format('S', 0, 0, w=DAT_WIDTH))
                        base_date_time = datetime.strptime(ts_start, '%Y-%m-%d %H:%M:%S')
                        for step in tidal_timeseries:
                            hours_so_far = (step[0] - base_date_time)
                            hours_so_far = 24 * hours_so_far.days + hours_so_far.seconds / (60 * 60)
                            lines.append('{0} {1:{w}} {2:{w}{b}}\n'
                                         .format('S', int(hours_so_far), float(step[1]), b='.2f', w=DAT_WIDTH))
        outflow_file.writelines(lines)
        outflow_file.close()
        print('Finished writing OUTFLOW.DAT')

