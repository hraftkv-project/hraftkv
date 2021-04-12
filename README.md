# HRaftKV

## Environment

* Tested on Ubuntu 16.04 and 20.04 LTS

## Build

* Build and install [brpc](https://github.com/brpc/brpc/blob/master/docs/cn/getting_started.md) which is the main dependency of HRaftKV

  ```shell
  $ sudo apt-get install -y git g++ make cmake libssl-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev
  $ git clone https://github.com/apache/incubator-brpc.git
  $ cd incubator-brpc
  $ mkdir build && cd build && cmake .. && make
  $ sudo make install
  ```

* Install the dependencies of HRaftKV

  ```shell
  $ sudo apt-get install -y cmake libsnappy-dev librocksdb-dev libboost-all-dev
  ```

* Compile HRaft that build atop Braft with cmake

  ```shell
  $ git clone https://github.com/hraftkv-project/hraftkv.git
  $ cd hraftkv
  $ mkdir build && cd build && cmake .. && make
  ```

* Compile HRaftKV

  ```shell
  $ cd example/hraftkv
  $ mkdir build && cd build && cmake .. && make
  ```

## Setup HRaftKV cluster

* Switch to the HRaftKV directory

  ```shell
  $ cd example/hraftkv
  ```

* Modify the server configuration file at `config/server.ini` to match your cluster settings

  * Settings specific to each server

    ```ini
    [server]
    # IP address of the server
    ip_addr = <server_ip>
    # Port of the server
    port = 8100
    # Replication index
    index = 0
    ```

  * Switch to enable or disable prioritized leader election

    ```ini
    [h-raft]
    # Enable prioritized leader election
    enable_ple = true
    # Enable prioritized leader election in PreVote
    enable_pre_vote_ple = true
    # Enable leadership transfer
    enable_leadership_transfer = true
    # Enable the differentiation of formal and probabtionary candidate
    enable_candidate_level = true
    ```

  * Dynamic server priority

    ```ini
    # Enable dynamic server priority
    enable_dynamic_priority = true
    # EWMA weight for new latency
    append_entries_ewma_weight = 0.2
    # EWMA update period (in ms)
    append_entries_ewma_timeout_ms = 1000
    # A threshold to update EWMA value, if the appending latency is lower than the threshold, do not update the EWMA value
    append_entries_latency_thresh_us = 1000
    # The mapping between appending latency and server priority
    # MAX stands for maximum value of the latency
    # e.g. For MAX:1,50000:2,30000:3,4000:4: 
    # > 50ms = 1, 30-50ms = 2, 4-30ms = 3, < 4ms = 4
    append_entries_latency_priority_map = MAX:1,50000:2,30000:3,4000:4
    ```

  * Multi-Raft

    ```ini
    # Enable Multi-Raft
    multi_raft_enable = true
    # Number of Raft groups (not include group_0)
    multi_raft_num_groups = 3
    ```

  * Raft group settings

    * No matter Multi-Raft is enabled or not, the configuration of `group_0` must be included in `server.ini`

    ```ini
    [group_0]
    # Group ID
    id = KVStore_0
    # List of raft peers
    # Note that the conf of group_0 must include all servers in the cluster
    # Format: <server_1's ip>:<server_1's port>:<server_1's index>,...
    conf = 192.168.10.26:8100:0,192.168.10.27:8100:0,192.168.10.28:8100:0
    # Follow the order of conf, e.g. the first value is the priority of server 1 in group_0
    initial_priority = 5,4,4,4,4
    initial_leader_priority = 5
    # Follow the order of conf, e.g. the first value is the election timeout of server 1 in group_0
    election_timeout_ms = 100,150,150,150,150
    ```

* Start HRaftKV on each server

  ```shell
  $ ./run_server.sh
  ```

* Stop HRaftKV

  ```shell
  $ ./stop.sh
  ```

## YCSB Client

