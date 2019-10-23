from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import requests


class GFSsensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        self.wrf_date = kwargs['wrf_date']
        self.gfs_hour = kwargs['gfs_hour']
        print("GFSsensor inputs: ", [self.wrf_date, self.gfs_hour])
        super(GFSsensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        try:
            print('-----------------------------------------------------------------------')
            wrf_date = self.wrf_date
            gfs_hour = self.gfs_hour
            result = requests.get(url=url)
            print('check_file_status|result : ', result.json())
            rest_data = result.json()
            print('-----------------------------------------------------------------------')
            return rest_data['completed']
        except AirflowSensorTimeout as to:
            self._do_skip_downstream_tasks(context)
            raise AirflowSensorTimeout('Snap. Time is OUT.')


