import argparse
from netCDF4 import Dataset
import numpy as np
import os
from datetime import datetime, timedelta
import pandas as pd

TMP_LOCATION = '/home/uwcc-admin/temp'

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


def dataframe_to_rfield(ts_df, rfield_file):
    ts_df.to_csv(rfield_file, columns=['Rain'], header=False, index=None)


def execute_cmd(cmd):
    print('execute_cmd|cmd : ', cmd)
    try:
        os.system(cmd)
    except Exception as e:
        print('execute_cmd|Exception : ', str(e))


def create_zip_file(wrf_id):
    print('create_zip_file|wrf_id : ', wrf_id)
    cmd = 'tar -czvf {}/{}/rfield.tar.gz {}/{}/rfield/*'.format(TMP_LOCATION, wrf_id,
                                                                                   TMP_LOCATION, wrf_id)
    execute_cmd(cmd)


def move_zip_file_to_bucket(wrf_id, bucket_path):
    print('move_zip_file_to_bucket|wrf_id : ', wrf_id)
    print('move_zip_file_to_bucket|bucket_path : ', bucket_path)
    cmd = 'mv {}/{}/rfield.tar.gz {}'.format(TMP_LOCATION, wrf_id, bucket_path)
    execute_cmd(cmd)


def remove_tmp_files(wrf_id):
    print('create_zip_file|wrf_id : ', wrf_id)
    cmd = 'rm -rf {}/{}'.format(TMP_LOCATION, wrf_id)
    execute_cmd(cmd)


def read_netcdf_file(netcdf_dir, rfield_dir, wrf_id):
    rainnc_net_cdf_file = os.path.join(netcdf_dir, 'd03_RAINNC.nc')
    if not os.path.exists(rainnc_net_cdf_file):
        print('no rainnc netcdf :: {}'.format(rainnc_net_cdf_file))
    else:
        try:
            create_dir_if_not_exists(rfield_dir)
            create_dir_if_not_exists(os.path.join(TMP_LOCATION, wrf_id))
            print('RAINNC netcdf data extraction')
            nnc_fid = Dataset(rainnc_net_cdf_file, mode='r')
            time_unit_info = nnc_fid.variables['XTIME'].units
            print('time_unit_info : ', time_unit_info)
            time_unit_info_list = time_unit_info.split('since ')
            print(time_unit_info_list)

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
            first = True
            for i in range(len(diff)):
                ts_time = datetime.strptime(time_unit_info_list[1], '%Y-%m-%d %H:%M:%S') + timedelta(
                    minutes=times[i + 1].item())
                t = datetime_utc_to_lk(ts_time, shift_mins=0)
                timestamp = t.strftime('%Y-%m-%d_%H-%M')
                ts_list = []
                for y in range(height):
                    for x in range(width):
                        lat = float('%.6f' % lats[y])
                        lon = float('%.6f' % lons[x])
                        rain = '%.3f' % float(diff[i, y, x])
                        ts_list.append([lon, lat, rain])
                ts_df = pd.DataFrame(ts_list, columns=['Lon', 'Lat', 'Rain'])
                if first:
                    x_y_file = os.path.join(rfield_dir, 'dwrf_x_y.csv')
                    ts_df.to_csv(x_y_file, columns=['Lon', 'Lat'], header=False, index=None)
                    first = False
                tmp_rfield_path = os.path.join(TMP_LOCATION, wrf_id, 'rfield')
                create_dir_if_not_exists(tmp_rfield_path)
                rfiled_file = os.path.join(tmp_rfield_path, 'dwrf_{}.txt'.format(timestamp))
                print(rfiled_file)
                ts_df.to_csv(rfiled_file, columns=['Rain'], header=False, index=None)
            create_zip_file(wrf_id)
            move_zip_file_to_bucket(wrf_id, rfield_dir)
            remove_tmp_files(wrf_id)
        except Exception as e:
            print("netcdf file at {} reading error.".format(rainnc_net_cdf_file))
            print('read_netcdf_file|Exception : ', str(e))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-wrf_id')
    parser.add_argument('-model')
    parser.add_argument('-version')
    parser.add_argument('-gfs_hour')
    parser.add_argument('-rfield_date')
    parser.add_argument('-base_dir')
    parser.add_argument('-wrf_run')
    return parser.parse_args()


# if __name__ == '__main__':
#     args = vars(parse_args())
#     wrf_id = args['wrf_id']
#     base_dir = args['base_dir']
#     input_list = wrf_id.split('_')
#     if len(input_list) >= 5:
#         version = input_list[1]
#         wrf_run = input_list[2]
#         gfs_hour = input_list[3]
#         rfield_date = input_list[4]
#         model = input_list[5]
#         print('---------User inputs---------')
#         print('model : ', model)
#         print('version : ', version)
#         print('gfs_hour : ', gfs_hour)
#         print('rfield_date : ', rfield_date)
#         print('wrf_run : ', wrf_run)
#         print('base_dir : ', base_dir)
#         print('wrf_id : ', wrf_id)
#         print('------------------------------')
#         netcdf_dir = os.path.join(base_dir, 'dwrf', version, 'd{}'.format(wrf_run), gfs_hour, rfield_date, model)
#         rfield_dir = os.path.join(base_dir, 'dwrf', version, 'd{}'.format(wrf_run), gfs_hour, rfield_date, model, 'rfield')
#         print('netcdf_dir : ', netcdf_dir)
#         print('rfield_dir : ', rfield_dir)
#         read_netcdf_file(netcdf_dir, rfield_dir, wrf_id)


if __name__ == '__main__':
    wrf_id = 'dwrf_4.0_d0_18_2019-10-17_E'
    base_dir = '/mnt/disks/wrf_nfs/'
    input_list = wrf_id.split('_')
    if len(input_list) >= 5:
        version = input_list[1]
        wrf_run = input_list[2]
        gfs_hour = input_list[3]
        rfield_date = input_list[4]
        model = input_list[5]
        print('---------User inputs---------')
        print('model : ', model)
        print('version : ', version)
        print('gfs_hour : ', gfs_hour)
        print('rfield_date : ', rfield_date)
        print('wrf_run : ', wrf_run)
        print('base_dir : ', base_dir)
        print('wrf_id : ', wrf_id)
        print('------------------------------')
        netcdf_dir = os.path.join(base_dir, 'dwrf', version, wrf_run, gfs_hour, rfield_date, model)
        rfield_dir = os.path.join(base_dir, 'dwrf', version, wrf_run, gfs_hour, rfield_date, model,
                                  'rfield')
        print('netcdf_dir : ', netcdf_dir)
        print('rfield_dir : ', rfield_dir)
        read_netcdf_file(netcdf_dir, rfield_dir, wrf_id)

# if __name__ == '__main__':
#     # nohup ./runner.sh -r 0 -m E -v 4.0 -h 18 &
#     # wrf_nfs / dwrf / 4.0 / d0 / 18 / 2019 - 10 - 17 / E
#     model = 'E'
#     version = '4.0'
#     gfs_hour = '18'
#     wrf_run = '0'
#     base_dir = '/mnt/disks/wrf_nfs/dwrf'
#     rfield_date = '2019-10-17'
#     # rfield_dir = os.path.join(base_dir, version, 'd{}'.format(wrf_run), gfs_hour, rfield_date, model, 'rfield', 'd03')
#     # netcdf_file_path = os.path.join(base_dir, version, 'd{}'.format(wrf_run), gfs_hour, rfield_date, model, 'd03_RAINNC.nc')
#     # create_dir_if_not_exists(rfield_dir)
#     read_netcdf_file('/home/hasitha/Desktop/dwrf_4.0_d0_18_2019-10-17_E_d03_RAINNC.nc',
#                      '/home/hasitha/Desktop/rfield_2019-10-17')