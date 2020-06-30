#!/usr/bin/env bash

echo `date`

while getopts ":r:m:v:d:h:i:" option; do
  case "${option}" in
  d) RUN_DATE=$OPTARG ;; # 2019-10-23
  r) WRF_RUN=$OPTARG ;; # 1/0
  m) MODEL=$OPTARG ;; # 1/0
  v) VERSION=$OPTARG ;; # 1/0
  h) GFS_HOUR=$OPTARG ;; # 00/06/12/18
  i) WRF_ID=$OPTARG ;; # 00/06/12/18
  esac
done

BASE_DIR='/mnt/disks/wrf_nfs/'

echo "RUN_DATE : $RUN_DATE"
echo "WRF_RUN : $WRF_RUN"
echo "GFS_HOUR : $GFS_HOUR"
echo "MODEL : $MODEL"
echo "VERSION : $VERSION"
echo "WRF_ID : $WRF_ID"

#if [ ${WRF_RUN} == 0 ] || [ ${WRF_RUN} == "0" ]; then
#    if [ -z "$RUN_DATE" ];then
#          tmp_date=`date '+%Y-%m-%d' --date="1 days ago"`
#          exec_date=`date '+%Y-%m-%d'`
#    else
#          tmp_date=$(date +%Y-%m-%d -d "${RUN_DATE} - 1 day")
#          exec_date=${RUN_DATE}
#    fi
#    gfs_date="${tmp_date}_${GFS_HOUR}:00"
#    wrf_id="dwrf_${VERSION}_${WRF_RUN}_${GFS_HOUR}_${exec_date}_${MODEL}"
#fi
#
#if [ ${WRF_RUN} == 1 ] || [ ${WRF_RUN} == "1" ]; then
#    if [ -z "$RUN_DATE" ];then
#          tmp_date=`date '+%Y-%m-%d'`
#          exec_date=`date '+%Y-%m-%d'`
#    else
#          tmp_date=${RUN_DATE}
#          exec_date=${RUN_DATE}
#    fi
#    gfs_date="${tmp_date}_${GFS_HOUR}:00"
#    wrf_id="dwrf_${VERSION}_${WRF_RUN}_${GFS_HOUR}_${exec_date}_${MODEL}"
#fi
#
#echo "gfs_date : ${gfs_date}"
#echo "exec_date : ${exec_date}"
#echo "run_id : ${run_id}"
#
#check_empty() {
#  [ -z "$1" ] && echo "" || echo "-$2=$1"
#}

#echo "Changing into /home/uwcc-admin/wrf_docker"
#cd /home/uwcc-admin/wrf_docker
#echo "Inside `pwd`"

#
## If no venv (python3 virtual environment) exists, then create one.
#if [ ! -d "venv" ]
#then
#    echo "Creating venv python3 virtual environment."
#    virtualenv -p python3 venv
#fi
#
## Activate venv.
#echo "Activating venv python3 virtual environment."
#source venv/bin/activate
#
## Install dependencies using pip.
#if [ ! -f "wrfv4_rfied.log" ]
#then
#    echo "Installing Pandas"
#    pip install pandas
#    echo "Installing netCDF4"
#    pip install netCDF4
#    touch wrfv4_rfied.log
#fi
#
## Run rfield generating scripts
#echo "Running wrfv4_rfield.py"
#python wrfv4_rfield.py $( check_empty "${run_id}" wrf_id )  \
#                            $( check_empty "${BASE_DIR}" base_dir ) >> docker_rfield.log 2>&1
#
## Deactivating virtual environment
#echo "Deactivating virtual environment"
#deactivate