#!/usr/bin/env bash

echo "#### Reading running args..."

HOME_DIR='/mnt/disks/data/wrf_run/wrf1'


while getopts ":h:d:c:v:r:m:" option; do
  case "${option}" in
  d) EXEC_DATE=$OPTARG ;; # 2019-09-24
  c) CHECK_GFS=$OPTARG ;; # true
  v) VERSION=$OPTARG ;; # 4.0
  r) RUN=$OPTARG ;; # 0 or 1
  h) HOUR=$OPTARG ;; # 00 or 06 or 12 or 18
  m) MODEL=$OPTARG ;; # A or C or E or SE
  esac
done

check_empty() {
  [ -z "$1" ] && echo "" || echo "-$2=$1"
}
echo "EXEC_DATE : $EXEC_DATE"
echo "HOME_DIR : $HOME_DIR"
echo "HOUR : $HOUR"
echo "CHECK_GFS : $CHECK_GFS"
echo "VERSION : $VERSION"
echo "RUN : $RUN"
echo "MODEL : $MODEL"

OUTPUT_DIR='${HOME_DIR}/${VERSION}/d${RUN}/${HOUR}/${MODEL}/${EXEC_DATE}'

echo "OUTPUT_DIR : $OUTPUT_DIR"

echo "#### Running WRF procedures..."
cd /home/Build_WRF/code
echo "Inside $(pwd)"
python3 wrfv4_run.py \
                    $( check_empty "$START_DATE" start_date ) \
                    $( check_empty "$MODE" mode ) \
                    $( check_empty "$RUN_ID" run_id )
echo "####WRF procedures completed"