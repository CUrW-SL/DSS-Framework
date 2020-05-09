#!/usr/bin/env bash

Xvfb :1 -screen 0 1024x768x24 &> xvfb.log &
export DISPLAY=:1.0
echo "Xvbf is running..."

echo "Changing into /home/curw/git/distributed_hechms"
cd /home/curw/git/distributed_hechms
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
echo "Installing Pandas"
pip install pandas
echo "Installing netCDF4"
pip install netCDF4
echo "Installing Flask"
pip install Flask
echo "Installing Flask-JSON"
pip install Flask-JSON
echo "Installing pandas"
pip install pandas
echo "Installing geopandas"
pip install geopandas
echo "Installing shapely"
pip install shapely
echo "Installing scipy"
pip install scipy
echo "Installing mysql-connector"
pip install mysql-connector
echo "Installing shapely"
pip install shapely
echo "Installing numpy"
pip install numpy
echo "Installing data layer"
pip install git+https://github.com/shadhini/curw_db_adapter.git
touch hechms.log

# Run rfield generating scripts
echo "Running HecHns Controller.py"
python controller.py >> hechms.log 2>&1