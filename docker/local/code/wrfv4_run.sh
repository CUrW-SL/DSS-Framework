#!/usr/bin/env bash

echo "#### Reading running args..."
while getopts ":d:i:m:g:k:v:a:b:" option; do
  case "${option}" in

  d) START_DATE=$OPTARG ;;
  k) RUN_ID=$OPTARG ;;
  m) MODE=$OPTARG ;;
  a) NAMELIST_WPS_CONTENT=$OPTARG ;;
  b) $NAMELIST_INPUT_CONTENT=$OPTARG ;;
  esac
done

check_empty() {
  [ -z "$1" ] && echo "" || echo "-$2=$1"
}
echo "START_DATE : $START_DATE"
echo "RUN_ID : $RUN_ID"
echo "MODE : $MODE"
echo "MODE : $MODE"
echo "MODE : $MODE"
echo "#### Running WRF procedures..."

ulimit -s unlimited

cd /home/Build_WRF/code
echo "Inside $(pwd)"
python3 wrfv4_run.py \
                    $( check_empty "$START_DATE" start_date ) \
                    $( check_empty "$MODE" mode ) \
                    $( check_empty "$RUN_ID" run_id )
                    $( check_empty "$NAMELIST_WPS_CONTENT" wps_content )
                    $( check_empty "$NAMELIST_INPUT_CONTENT" input_content )
echo "####WRF procedures completed"