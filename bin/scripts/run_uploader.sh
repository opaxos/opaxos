#!/bin/bash 
# script to run any linux command in all the machines
# usage:    ./run_uploader.sh <local_file_loc> <remote_destination>
# example:  ./run_uploader.sh tpcc_tracefile.csv /usr/local/griya/bin/tpcc_tracefile.csv

source variables.sh

upload_pids=()
for HOST in "${MACHINES[@]}"; do
    scp -i $SSH_KEY_LOC $1 $SSH_USERNAME@$HOST:$2 &
    upload_pids+=($!)
done
for pid in "${upload_pids[@]}"; do
    wait $pid
    echo "      upload process $pid exit with status $?"
done