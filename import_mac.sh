yell() { echo "`date -u +"%Y-%m-%dT%H:%M:%SZ"`: $*" >&2; }
die() { yell "$*"; exit 111; }
try() { "$@" || die "cannot $*"; }

  DATA="./data"
  OUTPUT="/data/prometheus_is"
  BIN="."
  TMP="/tmp"

echo $BIN

#!/bin/bash
mv $DATA/inbox/* $DATA/processing || die " No Files to process"

# generate the open metrics
try $BIN/json_to_influx $DATA/processing/*.json

# archive the processed files
try mv $DATA/processing/* $DATA/archive
