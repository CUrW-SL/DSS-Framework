#!/usr/bin/env bash

echo `date`

echo "Changing into /home/uwcc-admin/wrf_docker"
cd /home/uwcc-admin/wrf_docker
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
if [ ! -f "curw_sim_utils.log" ]
then
    echo "Installing Pandas"
    pip install pandas
    echo "Installing netCDF4"
    pip install netCDF4
    touch docker_rfield.log
fi


# Run rfield generating scripts
echo "Running update_obs_rainfall_flo2d_250.py"
python rain/flo2d_250/fcst/update_fcst_rainfall_flo2d_250.py >> rain/flo2d_250/fcst/curw_sim_fcst_flo2d_250.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate