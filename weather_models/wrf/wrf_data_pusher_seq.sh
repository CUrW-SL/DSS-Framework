#!/usr/bin/env bash

#!/usr/bin/env bash

# Print execution date time
echo `date`

echo "Changing into ~/wrf_data_pusher"
cd /home/curw/git/curw_wrf_data_pusher
echo "Inside `pwd`"


# If no venv (python3 virtual environment) exists, then create one.
if [ ! -d "venv" ]
then
    echo "Creating venv python3 virtual environment."
    virtualenv -p python3 venv
fi

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

# Install dependencies using pip.
if [ ! -f "db.log" ]
then
    echo "Installing numpy"
    pip install numpy
    echo "Installing netCDF4"
    pip install netCDF4
    echo "Installing cftime"
    pip install cftime
    echo "Installing PyMySQL"
    pip install PyMySQL
    echo "Installing PyYAML"
    pip install PyYAML
    echo "Installing data layer"
#    pip install git+https://github.com/shadhini/curw_db_adapter.git -U
    pip install git+https://github.com/shadhini/curw_db_adapter.git
fi

config_file_path=$1
wrf_root_directory=$2
gfs_run=$3
gfs_data_hour=$4
wrf_system=$5
date=$6
sim_tag=$7

## Push WRFv4 data into the database
echo "Running scripts to extract wrf data sequentially. Logs Available in wrf_data_pusher_seq.log file."
echo "Params passed :: config_file_path=$config_file_path, wrf_root_directory=$wrf_root_directory, gfs_run=$gfs_run,
gfs_data_hour=$gfs_data_hour, wrf_system=$wrf_system, date=$date"
./wrf_data_pusher_seq.py -c $config_file_path -d $wrf_root_directory -r $gfs_run -H $gfs_data_hour -s $wrf_system -D $date -a $sim_tag  >> wrf_data_pusher_seq.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
