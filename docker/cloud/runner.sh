#!/usr/bin/env bash

echo "#### Running WRF procedures..."

mkdir -p /mnt/disks/workspace1/wrf-data/gfs

while getopts ":r:m:v:d:h:" option; do
  case "${option}" in
  d) RUN_DATE=$OPTARG ;; # 2019-10-23
  r) WRF_RUN=$OPTARG ;; # 1/0
  m) MODEL=$OPTARG ;; # 1/0
  v) VERSION=$OPTARG ;; # 1/0
  h) GFS_HOUR=$OPTARG ;; # 00/06/12/18
  esac
done

echo "RUN_DATE : $RUN_DATE"
echo "WRF_RUN : $WRF_RUN"
echo "GFS_HOUR : $GFS_HOUR"
echo "MODEL : $MODEL"
echo "VERSION : $VERSION"


if [ ${WRF_RUN} == 0 ] || [ ${WRF_RUN} == "0" ]; then
    if [ -z "$RUN_DATE" ];then
          tmp_date=`date '+%Y-%m-%d' --date="1 days ago"`
          exec_date=`date '+%Y-%m-%d'`
    else
          tmp_date=$(date +%Y-%m-%d -d "${RUN_DATE} - 1 day")
          exec_date=${RUN_DATE}
    fi
    gfs_date="${tmp_date}_${GFS_HOUR}:00"
    wrf_id="dwrf_${VERSION}_${WRF_RUN}_${GFS_HOUR}_${exec_date}_${MODEL}"
fi

if [ ${WRF_RUN} == 1 ] || [ ${WRF_RUN} == "1" ]; then
    if [ -z "$RUN_DATE" ];then
          tmp_date=`date '+%Y-%m-%d'`
          exec_date=`date '+%Y-%m-%d'`
    else
          tmp_date=${RUN_DATE}
          exec_date=${RUN_DATE}
    fi
    gfs_date="${tmp_date}_${GFS_HOUR}:00"
    wrf_id="dwrf_${VERSION}_${WRF_RUN}_${GFS_HOUR}_${exec_date}_${MODEL}"
fi

docker_tag="wrf_${VERSION}_${MODEL}"

echo "tmp_date : ${tmp_date}"
echo "gfs_date : ${gfs_date}"
echo "exec_date : ${exec_date}"
echo "wrf_id : ${wrf_id}"
echo "docker_tag : ${docker_tag}"


docker run -i --rm --privileged -v /mnt/disks/workspace1/wrf-data/geog:/home/Build_WRF/geog \
    -v /home/uwcc-admin/uwcc-admin.json:/wrf/gcs.json curw-wrfv4:${docker_tag} \
    /home/Build_WRF/code/wrfv4_run.sh -d ${gfs_date} -k ${wrf_id} \
    -v wrf_nfs:/home/Build_WRF/nfs -v curwsl_archive_1:/home/Build_WRF/archive

#d03_netcdf_path="/mnt/disks/worker-disk/wrf_nfs/dwrf/${VERSION}/d${WRF_RUN}/${GFS_HOUR}/${exec_date}/${MODEL}/d03_RAINNC.nc"
d03_netcdf_path="/mnt/disks/worker-disk/wrf_nfs/wrf/${VERSION}/d${WRF_RUN}/${GFS_HOUR}/${exec_date}/output/dwrf/${MODEL}/d03_RAINNC.nc"
echo "d03_netcdf_path : ${d03_netcdf_path}"
