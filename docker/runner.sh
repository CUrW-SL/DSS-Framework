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

echo "EXEC_DATE : $EXEC_DATE"
echo "HOME_DIR : $HOME_DIR"
echo "HOUR : $HOUR"
echo "CHECK_GFS : $CHECK_GFS"
echo "VERSION : $VERSION"
echo "RUN : $RUN"
echo "MODEL : $MODEL"

OUTPUT_DIR='${HOME_DIR}/${VERSION}/d${RUN}/${HOUR}/${EXEC_DATE}/${MODEL}'
GFS_DIR='${HOME_DIR}/${VERSION}/d${RUN}/${HOUR}/${EXEC_DATE}/gfs'
ARCHIVE_DIR='${HOME_DIR}/${VERSION}/d${RUN}/${HOUR}/${EXEC_DATE}/${MODEL}/archive'

echo "OUTPUT_DIR : $OUTPUT_DIR"
echo "GFS_DIR : $GFS_DIR"
echo "ARCHIVE_DIR : $ARCHIVE_DIR"
