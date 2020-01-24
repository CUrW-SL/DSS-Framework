#!/usr/bin/env bash

echo "#### Reading running args..."
while getopts ":d:i:m:g:k:v:" option; do
  case "${option}" in

  d) START_DATE=$OPTARG ;;
  k) RUN_ID=$OPTARG ;;
  m) MODE=$OPTARG ;;
  esac
done

check_empty() {
  [ -z "$1" ] && echo "" || echo "-$2=$1"
}
echo "START_DATE : $START_DATE"
echo "RUN_ID : $RUN_ID"
echo "MODE : $MODE"
echo "#### Running WRF procedures..."

ulimit -s unlimited

cd /home/Build_WRF/code
echo "Inside $(pwd)"
python3 wrfv4_run.py \
                    $( check_empty "$START_DATE" start_date ) \
                    $( check_empty "$MODE" mode ) \
                    $( check_empty "$RUN_ID" run_id )
echo "####WRF procedures completed"