#!/usr/bin/env bash

echo "#### Running WRF procedures..."

mkdir -p /mnt/disks/workspace1/wrf-data/gfs

while getopts ":r:h:" option; do
  case "${option}" in
  r) WRF_RUN=$OPTARG ;; # 1/0
  h) GFS_HOUR=$OPTARG ;; # 00/06/12/18
  esac
done

echo "WRF_RUN : $WRF_RUN"
echo "GFS_HOUR : $GFS_HOUR"

if [ ${WRF_RUN} == 0 ] || [ ${WRF_RUN} == "0" ]; then
    echo ""
    tmp_date=`date '+%Y-%m-%d' --date="1 days ago"`
    gfs_date="${tmp_date}_18:00"
    exec_date=`date '+%Y-%m-%d'`
    wrf_id="wrfv4_${exec_date}"
fi

if [ ${WRF_RUN} == 1 ] || [ ${WRF_RUN} == "1" ]; then
    tmp_date=`date '+%Y-%m-%d'`
    gfs_date="${tmp_date}_18:00"
    exec_date=`date '+%Y-%m-%d'`
    wrf_id="wrfv4_${exec_date}"
fi

echo "gfs_date ${gfs_date}"
echo "exec_date ${exec_date}"
echo "wrf_id ${wrf_id}"

docker run -i --rm --privileged -v /mnt/disks/workspace1/wrf-data/geog:/home/Build_WRF/geog \
    -v /home/uwcc-admin/uwcc-admin.json:/wrf/gcs.json curw-wrfv4:ubuntu1604 \
    /home/Build_WRF/code/wrfv4_run.sh -d ${gfs_date} -k ${wrf_id} \
    -v curwsl_nfs_1:/home/Build_WRF/nfs -v curwsl_archive_1:/home/Build_WRF/archive

