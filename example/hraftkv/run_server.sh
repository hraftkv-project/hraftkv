#!/bin/bash

# import config
source ./config.sh

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

if [ "$FLAGS_clean" == "0" ]; then
    rm -rf runtime
fi

# create runtime directory
runtime_dir=$FLAGS_runtime_dir
echo "Using runtime_dir: $runtime_dir"
mkdir -p $runtime_dir
# copy server binary
cp ./kvstore_server $runtime_dir
# copy default server configuration file
cp ../config/server.ini $runtime_dir

# change to runtime directory
cd ${runtime_dir}
data_dir=${runtime_dir}/data

# create Rocksdb directory
mkdir -p ${runtime_dir}/${FLAGS_rocksdb_dir}

# Start server instance
if [ "${FLAGS_background}" -eq 1 ]
then
    echo "Execute in background"

    ${VALGRIND} ${PERF} ./kvstore_server \
    -data_path=${data_dir} \
    -raft_sync=${FLAGS_raft_sync} \
    -raft_log_storage_type=${FLAGS_raft_log_storage_type}
    > std.log 2>&1 &
else
    ${VALGRIND} ${PERF} ./kvstore_server \
    -data_path=${data_dir} \
    -raft_sync=${FLAGS_raft_sync} \
    -raft_log_storage_type=${FLAGS_raft_log_storage_type}
fi
