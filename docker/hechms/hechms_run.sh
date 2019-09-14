#!/usr/bin/env bash

Xvfb :1 -screen 0 1024x768x24 &> xvfb.log &
export DISPLAY=:1.0
echo "Xvbf is running..."

./hec-hms-421/hec-hms.sh &
echo "hec-hms is running..."

cd /home/hec-dssvue201
./hec-dssvue.sh &
#echo "hec-dssvue is running..."
cd /home

export AIRFLOW_HOME=/home/airflow
#mkdir ./OUTPUT

#workflow initdb
#sleep 5
#echo "Starting Airflow Webserver"
#workflow webserver -p 8080 &
/bin/bash