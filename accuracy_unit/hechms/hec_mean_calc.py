import pandas as pd
import pymysql
import sys

sys.path.insert(0, '/home/curw/git/DSS-Framework/accuracy_unit')
from db_plugin import get_discharge_obs_df, get_discharge_fcst_df

# from accuracy_unit.db_plugin import get_discharge_obs_df, get_discharge_fcst_df

# connection params
OBS_HOST = '35.227.163.211'
OBS_USER = 'admin'
OBS_PASSWORD = 'floody'
OBS_PORT = 3306
OBS_DB = 'curw_sim'

FCST_HOST = '192.168.1.43'
FCST_USER = 'root'
FCST_PASSWORD = 'cfcwm07'
FCST_PORT = 3306
FCST_DB = 'curw_fcst'


def calculate_hechms_model_mean(start_time, end_time):
    print('calculate_hechms_model_mean|[start_time, end_time]: ',[start_time, end_time])
    fcst_connection = pymysql.connect(host=FCST_HOST, user=FCST_USER, password=FCST_PASSWORD, db=FCST_DB,
                                      cursorclass=pymysql.cursors.DictCursor)
    obs_connection = pymysql.connect(host=OBS_HOST, user=OBS_USER, password=FCST_PASSWORD, db=OBS_DB,
                                     cursorclass=pymysql.cursors.DictCursor)
    obs_df = get_discharge_obs_df(obs_connection, start_time, end_time)
    if obs_df is not None and obs_df.empty is False:
        fcst_df = get_discharge_fcst_df(fcst_connection, start_time, end_time)
        if fcst_df is not None and fcst_df.empty is False:
            obs_cum_mean_df = obs_df.cumsum()
            fcst_cum_mean_df = fcst_df.cumsum()
            compare_cum_mean_df = pd.merge(obs_cum_mean_df, fcst_cum_mean_df, left_on='time',
                                           right_on='time')
            compare_cum_mean_df.observed = pd.to_numeric(compare_cum_mean_df.observed)
            compare_cum_mean_df.forecast = pd.to_numeric(compare_cum_mean_df.forecast)
            rmse = ((compare_cum_mean_df.observed - compare_cum_mean_df.forecast) ** 2).mean() ** .5
            rmse_params = {'rmse': rmse}
            print('calculate_hechms_model_mean|rmse_params : ', rmse_params)
            return rmse_params


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


if __name__ == '__main__':
    try:
        run_datetime = '2020-03-10 08:00:00'
        calculate_hechms_model_mean('2020-01-03 00:00:00', '2020-01-04 00:00:00')
    except Exception as e:
        print(str(e))
