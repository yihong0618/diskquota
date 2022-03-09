#!/usr/bin/env bash

gpdata_dir=$MASTER_DATA_DIRECTORY/../../
log_dirs=$(find $gpdata_dir -name "pg_log" -type d | awk -e '!/mirror/ && !/standby/' | sort)

for dir in $log_dirs; do
    log_file=$(ls -t1 $dir/gpdb* | head -n 1)
    cat "$log_file"
done
