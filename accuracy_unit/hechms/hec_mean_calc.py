import pandas as pd
import pymysql
import sys

# sys.path.insert(0, '/home/curw/git/DSS-Framework/accuracy_unit')
# from db_plugin import get_discharge_obs_df, get_discharge_fcst_df

from accuracy_unit.db_plugin import get_obs_water_levels, get_discharge_fcst_df

# from accuracy_unit.db_plugin import get_discharge_obs_df, get_discharge_fcst_df

# connection params
OBS_HOST = '35.227.163.211'
OBS_USER = 'admin'
OBS_PASSWORD = 'floody'
OBS_PORT = 3306
OBS_DB = 'curw_obs'

FCST_HOST = '35.227.163.211'
FCST_USER = 'admin'
FCST_PASSWORD = 'floody'
FCST_PORT = 6036
FCST_DB = 'curw_fcst'


def calculate_hechms_model_mean(start_time, end_time):
    print('calculate_hechms_model_mean|[start_time, end_time]: ',[start_time, end_time])
    fcst_connection = None
    obs_connection = None
    try:
        fcst_connection = pymysql.connect(host=FCST_HOST, user=FCST_USER, password=FCST_PASSWORD, db=FCST_DB,
                                          cursorclass=pymysql.cursors.DictCursor)
        obs_connection = pymysql.connect(host=OBS_HOST, user=OBS_USER, password=FCST_PASSWORD, db=OBS_DB,
                                         cursorclass=pymysql.cursors.DictCursor)
        obs_water_levels = get_obs_water_levels(obs_connection, start_time, end_time)
        obs_df = calculate_glencourse_discharge(obs_water_levels)
        if obs_df is not None and obs_df.empty is False:
            fcst_df = get_discharge_fcst_df(fcst_connection, start_time, end_time, sim_tag='hourly_run')
            if fcst_df is not None and fcst_df.empty is False:
                print('calculate_hechms_model_mean|obs_df : ', obs_df)
                print('calculate_hechms_model_mean|fcst_df : ', fcst_df)
                obs_cum_mean_df = obs_df.cumsum()
                print('calculate_hechms_model_mean|obs_cum_mean_df : ', obs_cum_mean_df)
                fcst_cum_mean_df = fcst_df.cumsum()
                print('calculate_hechms_model_mean|fcst_cum_mean_df : ', fcst_cum_mean_df)
                compare_cum_mean_df = pd.merge(obs_cum_mean_df, fcst_cum_mean_df, left_on='time',
                                               right_on='time')
                print('calculate_hechms_model_mean|compare_cum_mean_df : ', compare_cum_mean_df)
                compare_cum_mean_df.observed = pd.to_numeric(compare_cum_mean_df.observed)
                compare_cum_mean_df.forecast = pd.to_numeric(compare_cum_mean_df.forecast)
                rmse = ((compare_cum_mean_df.observed - compare_cum_mean_df.forecast) ** 2).mean() ** .5
                rmse_params = {'rmse': rmse}
                print('calculate_hechms_model_mean|rmse_params : ', rmse_params)
                return rmse_params
    except Exception as e:
        if obs_connection is not None:
            obs_connection.close()
        if fcst_connection is not None:
            fcst_connection.close()
        print('calculate_hechms_model_mean|db_connection|Exception : ', str(e))


def get_common_start_end(obs_cum_mean_df, fcst_cum_mean_df):
    if len(obs_cum_mean_df.index) - len(fcst_cum_mean_df.index) > 0:
        smallest_df = fcst_cum_mean_df
    else:
        smallest_df = obs_cum_mean_df
    print('get_common_start_end|smallest_df :', smallest_df)
    print('get_common_start_end|smallest_df.iloc[0] :', smallest_df.iloc[0])
    print('get_common_start_end|smallest_df.iloc[-1] :', smallest_df.iloc[-1])
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


if __name__ == '__main__':
    try:
        run_datetime = '2020-04-10 08:00:00'
        calculate_hechms_model_mean('2020-04-08 08:00:00', '2020-04-12 08:00:00')
    except Exception as e:
        print(str(e))
