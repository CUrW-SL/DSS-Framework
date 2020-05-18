#!/usr/bin/env bash

HOME_DIR="/mnt/disks/data/hechms"
#HOME_DIR="/mnt/disks/data/hechms/event"

while getopts ":d:f:b:r:p:D:T:u:x:y:z:m:n:" option; do
  case "${option}" in
  d) EXEC_DATE=$OPTARG ;; # 2019-09-24 10:30:00
  f) FORWARD=$OPTARG ;; # 3
  b) BACKWARD=$OPTARG ;; # 2
  r) INIT_RUN=$OPTARG ;; # 0 or 1
  p) POP_METHOD=$OPTARG ;; # MME
  D) DATE_ONLY=$OPTARG ;; # MME
  T) TIME_ONLY=$OPTARG ;; # MME
  u) DB_USER=$OPTARG ;; # MME
  x) DB_PWD=$OPTARG ;; # MME
  y) DB_HOST=$OPTARG ;; # MME
  z) DB_NAME=$OPTARG ;; # MME
  m) TARGET_MODEL=$OPTARG ;; # 'hechms_prod' / 'hechms_event'
  n) RUN_TYPE=$OPTARG ;; # 'event' / 'production'
  esac
done

echo "EXEC_DATE : $EXEC_DATE"
echo "FORWARD : $FORWARD"
echo "BACKWARD : $BACKWARD"
echo "INIT_RUN : $INIT_RUN"
echo "POP_METHOD : $POP_METHOD"
echo "DATE_ONLY : $DATE_ONLY"
echo "TIME_ONLY : $TIME_ONLY"
echo "DB_USER : $DB_USER"
echo "DB_HOST : $DB_HOST"
echo "DB_NAME : $DB_NAME"
echo "TARGET_MODEL : $TARGET_MODEL"
echo "RUN_TYPE : $RUN_TYPE"

#if [[ ${RUN_TYPE} == "event" ]]; then
#    OUTPUT_DIR="${HOME_DIR}/$RUN_TYPE/${DATE_ONLY}/${TIME_ONLY}"
#else
#    OUTPUT_DIR="${HOME_DIR}/$RUN_TYPE/${DATE_ONLY}/${TIME_ONLY}"
#fi

OUTPUT_DIR="${HOME_DIR}/${RUN_TYPE}/${DATE_ONLY}/${TIME_ONLY}"
mkdir -p ${OUTPUT_DIR}
echo "OUTPUT_DIR : $OUTPUT_DIR"

docker run -i --rm --privileged \
    -v ${OUTPUT_DIR}:/home/curw/git/distributed_hechms/output  \
    curw-hechms-v2-centos7:hechms_4.2.1  /home/curw/hechms_run.sh -d ${EXEC_DATE} \
    -f ${FORWARD} -b ${BACKWARD} -r ${INIT_RUN} -p ${POP_METHOD} \
    -u ${DB_USER} -x ${DB_PWD} -y ${DB_HOST} -z ${DB_NAME} -m ${TARGET_MODEL}

