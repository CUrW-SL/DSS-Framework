#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS=/wrf/gcs.json

echo "#### Reading running args..."
while getopts ":m:g:w:k:v:" option
do
 case "${option}"
 in
 m) MODEL=$OPTARG;;
 g) GFS_HOUR=$OPTARG;;
 g) WRF_CONFIG=$OPTARG;;
 k) GCS_KEY=$OPTARG
    if [ -f "$GCS_KEY" ]; then
        echo "#### using GCS KEY file location"
        cat "$GCS_KEY" > ${GOOGLE_APPLICATION_CREDENTIALS}
    else
        echo "#### using GCS KEY file content"
        echo "$GCS_KEY" > ${GOOGLE_APPLICATION_CREDENTIALS}
    fi
    ;;
 v) bucket=$(echo "$OPTARG" | cut -d':' -f1)
    path=$(echo "$OPTARG" | cut -d':' -f2)
    echo "#### mounting $bucket to $path"
    gcsfuse "$bucket" "$path" ;;
 esac
done

cd /wrf || exit

encode_base64 () {
    [ -z "$1" ] && echo "" || echo "-$2=$( echo "$1" | base64  --wrap=0 )"
}

check_empty () {
        [ -z "$1" ] && echo "" || echo "-$2=$1"
}

echo "#### Running WRF procedures..."
python3.6 /docker/wrf/run_wrf.py \
                                $( check_empty "$MODEL" wrf_model ) \
                                $( check_empty "GFS_HOUR" gfs_hour ) \
                                $( check_empty "$WRF_CONFIG" wrf_config )

