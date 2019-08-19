import os
import traceback
from netCDF4._netCDF4 import Dataset

if __name__ == "__main__":
    net_cdf_file_path = '/home/hasitha/PycharmProjects/DSS-Framework/input/wrfout_d03_2019-08-12_18_00_00_rf.nc'
    try:
        if os.path.exists(net_cdf_file_path) and os.path.exists(net_cdf_file_path):
            nc_fid = Dataset(net_cdf_file_path, mode='r')
            print('net_cdf variables : ', nc_fid.variables.keys())
            rainc_unit_info = nc_fid.variables['RAINC'].units
            print('unit_info: ', rainc_unit_info)
            lat_unit_info = nc_fid.variables['XLAT'].units
            print('lat_unit_info: ', lat_unit_info)
            time_unit_info = nc_fid.variables['Times'].units
            print('time_unit_info: ', time_unit_info)
            time_unit_info_list = time_unit_info.split(' ')
    except Exception as e:
        print('Read netcdf info exception|e: ', str(e))
        traceback.print_exc()