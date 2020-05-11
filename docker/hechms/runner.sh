#!/usr/bin/env bash

HOME_DIR="/mnt/disks/data/hechms/event"

while getopts ":d:f:b:r:p:D:T:" option; do
  case "${option}" in
  d) EXEC_DATE=$OPTARG ;; # 2019-09-24 10:30:00
  f) FORWARD=$OPTARG ;; # 3
  b) BACKWARD=$OPTARG ;; # 2
  r) INIT_RUN=$OPTARG ;; # 0 or 1
  p) POP_METHOD=$OPTARG ;; # MME
  D) DATE_ONLY=$OPTARG ;; # MME
  T) TIME_ONLY=$OPTARG ;; # MME
  esac
done

echo "EXEC_DATE : $EXEC_DATE"
echo "FORWARD : $FORWARD"
echo "BACKWARD : $BACKWARD"
echo "INIT_RUN : $INIT_RUN"
echo "POP_METHOD : $POP_METHOD"
echo "DATE_ONLY : $DATE_ONLY"
echo "TIME_ONLY : $TIME_ONLY"

OUTPUT_DIR="${HOME_DIR}/${DATE_ONLY}/${TIME_ONLY}"
mkdir -p ${OUTPUT_DIR}
echo "OUTPUT_DIR : $OUTPUT_DIR"

docker run -i --rm --privileged \
    -v ${OUTPUT_DIR}:/home/curw/git/distributed_hechms/output  \
    curw-hechms-centos7:hechms_4.2.1  /home/curw/hechms_run.sh -d ${EXEC_DATE} -f ${FORWARD} -b ${BACKWARD} -r ${INIT_RUN} -p ${POP_METHOD}

