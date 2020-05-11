#!/usr/bin/env bash

while getopts ":d:f:b:r:p:" option; do
  case "${option}" in
  d) EXEC_DATE=$OPTARG ;; # 2019-09-24 10:30:00
  f) FORWARD=$OPTARG ;; # 3
  b) BACKWARD=$OPTARG ;; # 2
  r) INIT_RUN=$OPTARG ;; # 0 or 1
  p) POP_METHOD=$OPTARG ;; # MME
  esac
done

check_empty() {
  [ -z "$1" ] && echo "" || echo "-$2=$1"
}
echo "EXEC_DATE : $EXEC_DATE"
echo "FORWARD : $FORWARD"
echo "BACKWARD : $BACKWARD"
echo "INIT_RUN : $INIT_RUN"
echo "POP_METHOD : $POP_METHOD"


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
echo "Running HEC-HMS Model."
python hechms_workflow.py \
                    $( check_empty "$EXEC_DATE" run_datetime ) \
                    $( check_empty "$FORWARD" forward ) \
                    $( check_empty "$BACKWARD" backward ) \
                    $( check_empty "$INIT_RUN" init_run ) \
                    $( check_empty "$POP_METHOD" pop_method )
echo "####HEC-HMS procedures completed"

deactivate
echo "####HEC-HMS venv deactivated"
