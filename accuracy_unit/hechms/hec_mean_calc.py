import argparse
import json

import pandas as pd
import pymysql
import sys
from datetime import datetime, timedelta
from memory_profiler import memory_usage, profile

# sys.path.insert(0, '/home/curw/git/DSS-Framework/accuracy_unit')
sys.path.insert(0, '/home/hasitha/PycharmProjects/DSS-Framework/accuracy_unit')
from db_plugin import get_obs_water_levels, get_discharge_fcst_df, get_discharge_obs_df


# connection params
OBS_HOST = '35.197.98.125'
OBS_USER = 'admin'
OBS_PASSWORD = 'floody'
OBS_PORT = 3306
OBS_DB = 'curw_obs'

FCST_HOST = '35.197.98.125'
FCST_USER = 'admin'
FCST_PASSWORD = 'floody'
FCST_PORT = 6036
FCST_DB = 'curw_fcst'

SIM_HOST = '35.197.98.125'
SIM_USER = 'admin'
SIM_PASSWORD = 'floody'
SIM_PORT = 6036
SIM_DB = 'curw_sim'


@profile
def calculate_hechms_model_mean(start_time, end_time, source_ids):
    print('calculate_hechms_model_mean|[start_time, end_time, source_ids]: ', [start_time, end_time, source_ids])
    fcst_connection = None
    obs_connection = None
    sim_connection = None
    try:
        fcst_connection = pymysql.connect(host=FCST_HOST, user=FCST_USER, password=FCST_PASSWORD, db=FCST_DB,
                                          cursorclass=pymysql.cursors.DictCursor)
        obs_connection = pymysql.connect(host=OBS_HOST, user=OBS_USER, password=FCST_PASSWORD, db=OBS_DB,
                                         cursorclass=pymysql.cursors.DictCursor)
        sim_connection = pymysql.connect(host=SIM_HOST, user=SIM_USER, password=SIM_PASSWORD, db=SIM_DB,
                                         cursorclass=pymysql.cursors.DictCursor)
        # obs_water_levels = get_obs_water_levels(obs_connection, start_time, end_time)
        # obs_df = calculate_glencourse_discharge(obs_water_levels)
        obs_df = get_discharge_obs_df(sim_connection, start_time, end_time)
        if obs_df is not None and obs_df.empty is False:
            for source_id in source_ids:
                fcst_df = get_discharge_fcst_df(fcst_connection, start_time, end_time, source_id, sim_tag='hourly_run')
                if fcst_df is not None and fcst_df.empty is False:
                    # print('calculate_hechms_model_mean|obs_df : ', obs_df)
                    # print('calculate_hechms_model_mean|fcst_df : ', fcst_df)
                    obs_cum_mean_df = obs_df.cumsum()
                    # print('calculate_hechms_model_mean|obs_cum_mean_df : ', obs_cum_mean_df)
                    fcst_cum_mean_df = fcst_df.cumsum()
                    # print('calculate_hechms_model_mean|fcst_cum_mean_df : ', fcst_cum_mean_df)
                    compare_cum_mean_df = pd.merge(obs_cum_mean_df, fcst_cum_mean_df, left_on='time',
                                                   right_on='time')
                    # print('calculate_hechms_model_mean|compare_cum_mean_df : ', compare_cum_mean_df)
                    compare_cum_mean_df.observed = pd.to_numeric(compare_cum_mean_df.observed)
                    compare_cum_mean_df.forecast = pd.to_numeric(compare_cum_mean_df.forecast)
                    rmse = ((compare_cum_mean_df.observed - compare_cum_mean_df.forecast) ** 2).mean() ** .5
                    rmse_params = {'source_id':source_id, 'rmse': rmse}
                    print('calculate_hechms_model_mean|rmse_params : ', rmse_params)
    except Exception as e:
        print('calculate_hechms_model_mean|db_connection|Exception : ', str(e))
    finally:
        if obs_connection is not None:
            obs_connection.close()
        if fcst_connection is not None:
            fcst_connection.close()
        if sim_connection is not None:
            sim_connection.close()


def get_common_start_end(obs_cum_mean_df, fcst_cum_mean_df):
    if len(obs_cum_mean_df.index) - len(fcst_cum_mean_df.index) > 0:
        smallest_df = fcst_cum_mean_df
    else:
        smallest_df = obs_cum_mean_df
    # print('get_common_start_end|smallest_df :', smallest_df)
    # print('get_common_start_end|smallest_df.iloc[0] :', smallest_df.iloc[0])
    # print('get_common_start_end|smallest_df.iloc[-1] :', smallest_df.iloc[-1])
    start = smallest_df.iloc[0]['time']
    end = smallest_df.iloc[-1]['time']
    obs_cum_mean_df1 = obs_cum_mean_df[obs_cum_mean_df['time'] >= start]
    obs_cum_mean_df2 = obs_cum_mean_df1[obs_cum_mean_df1['time'] <= end]
    fcst_cum_mean_df1 = fcst_cum_mean_df[fcst_cum_mean_df['time'] >= start]
    fcst_cum_mean_df2 = fcst_cum_mean_df1[fcst_cum_mean_df1['time'] <= end]
    return [obs_cum_mean_df2, fcst_cum_mean_df2]


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
        # print('calculate_glencourse_discharge|glencourse_wl_ts[i] : ', glencourse_wl_ts[i])
        wl = float(glencourse_wl_ts[i]['value'])
        if wl <= 16:
            discharge = 41.904 * ((wl - 7.65) ** 1.518)
        else:
            discharge = 21.323 * ((wl - 7.71) ** 1.838)
        discharge_ts.append({'time': glencourse_wl_ts[i]['time'], 'value': discharge})
    print('calculate_glencourse_discharge|discharge_ts : ', discharge_ts)
    df = pd.DataFrame(data=discharge_ts, columns=['time', 'value']).set_index(keys='time')
    df.rename(columns={'value': 'observed'}, inplace=True)
    return df


def get_ts_start_end(run_date, run_time, forward=3, backward=2):
    result = {}
    run_datetime = datetime.strptime('%s %s' % (run_date, '00:00:00'), '%Y-%m-%d %H:%M:%S')
    ts_start_datetime = run_datetime - timedelta(days=backward)
    ts_end_datetime = run_datetime + timedelta(days=forward)
    run_datetime = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    result['ts_start'] = ts_start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    result['run_time'] = run_datetime.strftime('%Y-%m-%d %H:%M:%S')
    result['ts_end'] = ts_end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    return result


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-run_date')
    parser.add_argument('-run_time')
    return parser.parse_args()


if __name__ == '__main__':
    try:
        args = vars(parse_args())
        print('Running arguments:\n%s' % json.dumps(args, sort_keys=True, indent=0))
        run_date = args['run_date']
        run_time = args['run_time']
        # run_date = '2020-06-24'
        # run_time = '00:00:00'
        time_gaps = get_ts_start_end(run_date, run_time)
        source_ids = [17, 25, 27, 28]
        calculate_hechms_model_mean(time_gaps['ts_start'], time_gaps['run_time'], source_ids)
    except Exception as e:
        print(str(e))
