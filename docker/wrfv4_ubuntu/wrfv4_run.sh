#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS=/home/Build_WRF/code/gcs.json

echo "#### Reading running args..."
while getopts ":d:i:g:k:v:" option; do
  case "${option}" in

  d) START_DATE=$OPTARG ;;
  k) RUN_ID=$OPTARG ;;
  v)
    bucket=$(echo "$OPTARG" | cut -d':' -f1)
    path=$(echo "$OPTARG" | cut -d':' -f2)
    echo "#### mounting $bucket to $path"
    gcsfuse "$bucket" "$path"
    ;;
  esac
done

check_empty() {
  [ -z "$1" ] && echo "" || echo "-$2=$1"
}

echo "#### Running WRF procedures..."
echo "Inside $(pwd)"

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate
python3 wrfv4_run.py \
                    $( check_empty "$START_DATE" start_date ) \
                    $( check_empty "$RUN_ID" run_id )
echo "Deactivating virtual environment"
deactivate