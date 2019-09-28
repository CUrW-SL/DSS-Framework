#!/usr/bin/env bash

echo "#### Reading running args..."

HOME_DIR="/mnt/disks/data/wrf_run/wrf1"
GFS_URL="ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{RUN_DATE}/{DATA_HOUR}"
GFS_DOWNLOAD_FILE_TEMPLATE="gfs.t{DATA_HOUR}z.pgrb2.0p50.f0{INDEX}"

while getopts ":h:d:t:c:s:r:m:" option; do
  case "${option}" in
  d) EXEC_DATE=$OPTARG ;; # 2019-09-24
  t) EXEC_TIME=$OPTARG ;; # 04:00
  c) OVERWRITE=$OPTARG ;; # true
  s) VERSION=$OPTARG ;; # 4.0
  r) RUN=$OPTARG ;; # 0 or 1
  h) HOUR=$OPTARG ;; # 00 or 06 or 12 or 18
  m) MODEL=$OPTARG ;; # A or C or E or SE
  esac
done

echo "EXEC_DATE : $EXEC_DATE"
echo "EXEC_TIME : $EXEC_TIME"
echo "HOME_DIR : $HOME_DIR"
echo "HOUR : $HOUR"
echo "OVERWRITE : $OVERWRITE"
echo "VERSION : $VERSION"
echo "RUN : $RUN"
echo "MODEL : $MODEL"

OUTPUT_DIR="${HOME_DIR}/${VERSION}/d${RUN}/${HOUR}/${EXEC_DATE}/${MODEL}"
GFS_DIR="${HOME_DIR}/${VERSION}/d${RUN}/${HOUR}/${EXEC_DATE}/gfs"
ARCHIVE_DIR="${HOME_DIR}/${VERSION}/d${RUN}/${HOUR}/${EXEC_DATE}/${MODEL}/archive"
START_DATE="${EXEC_DATE}_${EXEC_TIME}"

mkdir -p ${OUTPUT_DIR}
echo "OUTPUT_DIR : $OUTPUT_DIR"
mkdir -p ${GFS_DIR}
echo "GFS_DIR : $GFS_DIR"
mkdir -p ${ARCHIVE_DIR}
echo "ARCHIVE_DIR : $ARCHIVE_DIR"

docker run -i --rm --privileged \
    -v /mnt/disks/data/samba/wrf-static-data/geog:/home/Build_WRF/geog  \
    -v ${OUTPUT_DIR}:/home/Build_WRF/nfs  \
    -v ${GFS_DIR}:/home/Build_WRF/gfs  \
    -v ${ARCHIVE_DIR}:/home/Build_WRF/archive \
    curw-wrfv4:ubuntu1604  /home/Build_WRF/code/wrfv4_run.sh -d ${START_DATE} -h ${HOUR} -c ${OVERWRITE}  -s ${VERSION} -r ${RUN} -m ${MODEL} -u ${GFS_URL}