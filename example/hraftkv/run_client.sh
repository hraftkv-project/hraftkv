#!/bin/bash

# import config
source ./config.sh

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

# The alias for printing to stderr
alias error=">&2 echo kvstore: "

# decide what mode it will be using
option=$2

if [ "$FLAGS_local_mode" == "1" ]; then 
    echo "running in local mode"
    local_ip="127.0.0.1"
    # create raft peer list
    ## example: 192.168.0.32:8100:0,192.168.0.32:8100:0,192.168.0.32:8100:0
    ## assume all servers use same port
    raft_peers=""
    for ((i=0; i<${#peers_ip[@]}; ++i)); do
        port=$((${FLAGS_port} + i))
        raft_peers="${raft_peers}${local_ip}:${port}:0,"
    done
else 
    echo "running in distributed mode"
    # create raft peer list
    ## example: 192.168.0.32:8100:0,192.168.0.32:8100:0,192.168.0.32:8100:0
    ## assume all servers use same port
    raft_peers=""
    for ((i=0; i<${#peers_ip[@]}; ++i)); do
        raft_peers="${raft_peers}${peers_ip[$i]}:${FLAGS_port}:0,"
    done
fi

# execute instance
echo "raft_peers: $raft_peers"
${VALGRIND} ./kvstore_client -conf=${raft_peers}
