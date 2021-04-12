#!/bin/bash

# import config
source ./config.sh

# path
# runtime_dir=$RUNTIME_DIR
echo "Removing runtime_dir: $FLAGS_runtime_dir"

# stop server instance
killall -9 kvstore_server

# remove runtime directory
rm -r ${FLAGS_runtime_dir}
