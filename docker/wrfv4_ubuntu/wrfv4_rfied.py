import traceback
from netCDF4 import Dataset
import numpy as np
import os
from datetime import datetime, timedelta
import pandas as pd


def get_per_time_slot_values(prcp):
    per_interval_prcp = (prcp[1:] - prcp[:-1])
    return per_interval_prcp


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


def write_to_file(file_name, data):
    with open(file_name, 'w') as f:
        for _string in data:
            f.write(str(_string) + '\n')
        f.close()


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def read_netcdf_file(rainnc_net_cdf_file):
    if not os.path.exists(rainnc_net_cdf_file):
        print('no rainnc netcdf :: {}'.format(rainnc_net_cdf_file))
    else:
        try:
            print('RAINNC netcdf data extraction')
            nnc_fid = Dataset(rainnc_net_cdf_file, mode='r')
            time_unit_info = nnc_fid.variables['XTIME'].units
            print('time_unit_info : ', time_unit_info)
            time_unit_info_list = time_unit_info.split('since ')

            lats = nnc_fid.variables['XLAT'][0, :, 0]
            lons = nnc_fid.variables['XLONG'][0, 0, :]

            lon_min = lons[0].item()
            lat_min = lats[0].item()
            lon_max = lons[-1].item()
            lat_max = lats[-1].item()

            lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
            lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

            rainnc = nnc_fid.variables['RAINNC'][:, lat_inds[0], lon_inds[0]]
            times = nnc_fid.variables['XTIME'][:]
            nnc_fid.close()

            diff = get_per_time_slot_values(rainnc)
            width = len(lons)
            height = len(lats)

            timeseries_dict = {}

            # for i in range(len(diff)):
            #     for y in range(height):
            #         for x in range(width):
            #             lat = float('%.6f' % lats[y])
            #             lon = float('%.6f' % lons[x])

            x_y_list = []
            for y in range(height):
                for x in range(width):
                    lat = float('%.6f' % lats[y])
                    lon = float('%.6f' % lons[x])
                    x_y_list.append([lon, lat])
            x_y_df = pd.DataFrame(x_y_list, columns=['Lon', 'Lat'])
            print('x_y_df : ', x_y_df)
                    # station = '{} {}'.format(lon, lat)
                    # data_list = []
                    # for i in range(len(diff)):
                    #     data_list.append('%.3f' % float(diff[i, y, x]))
                    # timeseries_dict[station] = data_list

            # for i in range(len(diff)):
            #     rfield = []
            #     ts_time = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S') + timedelta(
            #         minutes=times[i + 1].item())
            #     t = datetime_utc_to_lk(ts_time, shift_mins=0)
            #     timestamp = t.strftime('%Y-%m-%d_%H-%M')
            #     for station in timeseries_dict.keys():
            #         rfield.append('{} {}'.format(station, timeseries_dict.get(station)[i]))
            #     write_to_file(
            #         '/home/hasitha/Desktop/rfield/{}_{}_{}_rfield.txt'.format(model, version, timestamp), rfield)
        except Exception as e:
            print("netcdf file at {} reading error.".format(rainnc_net_cdf_file))
            traceback.print_exc()


if __name__ == '__main__':
    # nohup ./runner.sh -r 0 -m E -v 4.0 -h 18 &
    model = 'E'
    version = '4.0'
    gfs_hour = '18'
    wrf_run = '0'
    base_dir = '/mnt/disks/wrf_nfs/dwrf'
    read_netcdf_file('/home/hasitha/Desktop/dwrf_4.0_d0_18_2019-10-16_C_d03_RAINNC.nc')

