#!/usr/bin/env bash

echo "#### Reading running args..."
while getopts "d:i:m:g:k:v:a:b:p:q:r:s:" option; do
  case "${option}" in
  d) START_DATE=$OPTARG ;;
  k) RUN_ID=$OPTARG ;;
  m) MODE=$OPTARG ;;
  a) NAMELIST_WPS_ID=$OPTARG ;;
  b) NAMELIST_INPUT_ID=$OPTARG ;;
  p) DB_USER=$OPTARG ;; # 2
  q) DB_PASSWORD=$OPTARG ;; # 2
  r) DB_NAME=$OPTARG ;; # 2
  s) DB_HOST=$OPTARG ;; # 2
  esac
done

check_empty() {
  [ -z "$1" ] && echo "" || echo "-$2=$1"
}
echo "START_DATE : $START_DATE"
echo "RUN_ID : $RUN_ID"
echo "MODE : $MODE"
echo "NAMELIST_WPS_ID : $NAMELIST_WPS_ID"
echo "NAMELIST_INPUT_ID : $NAMELIST_INPUT_ID"
echo "DB_USER : $DB_USER"
echo "DB_PASSWORD : $DB_PASSWORD"
echo "DB_NAME : $DB_NAME"
echo "DB_HOST : $DB_HOST"
echo "#### Running WRF procedures..."

ulimit -s unlimited

cd /home/Build_WRF/code
echo "Inside $(pwd)"
python3 wrfv4_run.py \
                    $( check_empty "$START_DATE" start_date ) \
                    $( check_empty "$MODE" mode ) \
                    $( check_empty "$RUN_ID" run_id ) \
                    $( check_empty "$NAMELIST_WPS_ID" wps_config_id ) \
                    $( check_empty "$NAMELIST_INPUT_ID" input_config_id ) \
                    $( check_empty "$DB_USER" db_user )
                    $( check_empty "$DB_PASSWORD" db_password )
                    $( check_empty "$DB_NAME" db_name )
                    $( check_empty "$DB_HOST" db_host )
echo "####WRF procedures completed"