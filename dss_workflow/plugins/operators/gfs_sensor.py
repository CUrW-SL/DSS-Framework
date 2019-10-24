from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from datetime import datetime
from ftplib import FTP


def check_gfs_data(gfs_hour,
                   last_gfs_file_format='gfs.t{}z.pgrb2.0p50.f075',
                   check_dt=None,
                   gfs_server='ftp.ncep.noaa.gov',
                   gfs_home='/pub/data/nccf/com/gfs/prod'):
    """
    :param check_dt: string 2019-10-21 11:43:00
    :return:
    """
    gfs_data_downloadable = False
    print('check_gfs_data|check_dt: ', check_dt)
    print('check_gfs_data|gfs_server: ', gfs_server)
    print('check_gfs_data|gfs_home: ', gfs_home)
    print('check_gfs_data|last_gfs_file_format: ', last_gfs_file_format)
    if check_dt is not None:
        gfs_dt = datetime.strptime(check_dt, '%Y-%m-%d %H:%M:%S')
    else:
        gfs_dt = datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    print('check_gfs_data|gfs_dt: ', gfs_dt)
    ftp = FTP(gfs_server)
    ftp.login()
    gfs_date = gfs_dt.strftime('%Y%m%d')
    ftp_cwd = '{}/gfs.{}/{}/'.format(gfs_home, gfs_date, gfs_hour)
    print('check_gfs_data|ftp_cwd: ', ftp_cwd)
    try:
        change_dir = ftp.cwd(ftp_cwd)
        print('change_dir : ', change_dir)
        last_gfs_file = last_gfs_file_format.format(gfs_hour)
        print('last_gfs_file : ', last_gfs_file)
        file_list = ftp.nlst()
        if len(file_list) > 0:
            for gfs_file in file_list:
                if gfs_file == last_gfs_file:
                    gfs_data_downloadable = True
    except Exception as e:
        print('change_dir|Exception : ', str(e))
    finally:
        print('check_gfs_data|gfs_data_downloadable : ', gfs_data_downloadable)
        return gfs_data_downloadable


class GfsSensorOperator(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        self.wrf_date = kwargs['wrf_date']
        self.gfs_hour = kwargs['gfs_hour']
        print("GFSsensor inputs: ", [self.wrf_date, self.gfs_hour])
        super(GfsSensorOperator, self).__init__(*args, **kwargs)

    def poke(self, context):
        try:
            print('-----------------------------------------------------------------------')
            condition = check_gfs_data(self.gfs_hour, check_dt=self.wrf_date)
            print('GFSsensor|condition : ', condition)
            print('-----------------------------------------------------------------------')
            return condition
        except AirflowSensorTimeout as to:
            self._do_skip_downstream_tasks(context)
            raise AirflowSensorTimeout('GFSsensor. Time is OUT.')


class GfsSensor(AirflowPlugin):
    name = "conditional_multi_trigger_operator"
    operators = [GfsSensorOperator]
