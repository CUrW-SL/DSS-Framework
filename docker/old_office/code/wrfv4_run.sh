#!/usr/bin/env bash

echo "#### Reading running args..."


while getopts ":h:d:c:s:r:m:u:" option; do
  case "${option}" in
  d) EXEC_DATE=$OPTARG ;; # 2019-09-24
  c) OVERWRITE=$OPTARG ;; # true
  s) VERSION=$OPTARG ;; # 4.0
  r) RUN=$OPTARG ;; # 0 or 1
  h) HOUR=$OPTARG ;; # 00 or 06 or 12 or 18
  m) MODEL=$OPTARG ;; # A or C or E or SE
  U) GFS_URL=$OPTARG ;; # A or C or E or SE
  esac
done

check_empty() {
  [ -z "$1" ] && echo "" || echo "-$2=$1"
}
echo "EXEC_DATE : $EXEC_DATE"
echo "HOUR : $HOUR"
echo "OVERWRITE : $OVERWRITE"
echo "VERSION : $VERSION"
echo "RUN : $RUN"
echo "MODEL : $MODEL"
echo "GFS_URL : $GFS_URL"

echo "#### Running WRF procedures..."
cd /home/Build_WRF/code
echo "Inside $(pwd)"
python3 wrfv4_run.py \
                    $( check_empty "$EXEC_DATE" exec_date ) \
                    $( check_empty "$OVERWRITE" overwrite ) \
                    $( check_empty "$VERSION" version ) \
                    $( check_empty "$RUN" run ) \
                    $( check_empty "$HOUR" hour ) \
                    $( check_empty "$GFS_URL" gfs_url ) \
                    $( check_empty "$MODEL" model )
echo "####WRF procedures completed"