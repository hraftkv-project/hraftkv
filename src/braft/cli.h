// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#ifndef  BRAFT_CLI_H
#define  BRAFT_CLI_H

#include "braft/raft.h"
#include "braft/cli.pb.h"                // CliService_Stub

namespace braft {
namespace cli {

struct CliOptions {
    int timeout_ms;
    int max_retry;
    CliOptions() : timeout_ms(-1), max_retry(3) {}
};

// Add a new peer into the replicating group which consists of |conf|.
// Returns OK on success, error information otherwise.
butil::Status add_peer(const GroupId& group_id, const Configuration& conf,
                       const PeerId& peer_id, const CliOptions& options);

// Remove a peer from the replicating group which consists of |conf|.
// Returns OK on success, error information otherwise.
butil::Status remove_peer(const GroupId& group_id, const Configuration& conf,
                          const PeerId& peer_id, const CliOptions& options);

// Gracefully change the peers of the replication group.
butil::Status change_peers(const GroupId& group_id, const Configuration& conf, 
                           const Configuration& new_peers,
                           const CliOptions& options);

// Transfer the leader of the replication group to the target peer
butil::Status transfer_leader(const GroupId& group_id, const Configuration& conf,
                              const PeerId& peer, const CliOptions& options);

// Reset the peer set of the target peer
butil::Status reset_peer(const GroupId& group_id, const PeerId& peer_id,
                         const Configuration& new_conf,
                         const CliOptions& options);

// Ask the peer to dump a snapshot immediately.
butil::Status snapshot(const GroupId& group_id, const PeerId& peer_id,
                       const CliOptions& options, SnapshotResponse *response);

// Transfer the leader of the replication group to the target peer
butil::Status transfer_leader(const GroupId& group_id, const Configuration& conf,
                              const PeerId& peer, const CliOptions& options);

butil::Status snapshot_on_leader(const GroupId& group_id, const Configuration conf, 
                                const CliOptions& options, SnapshotResponse *response);
                                
butil::Status get_all_priority(const GroupId& group_id, const Configuration conf, 
                                const CliOptions& options, GetAllPriorityResponse *response, PeerId *leader_id);
butil::Status get_leader_latency(const GroupId& group_id, const Configuration conf, 
                                const CliOptions& options, GetLeaderLatencyResponse *response, PeerId *leader_id);
// Benchmark
butil::Status bm_election_start(const GroupId& group_id, const Configuration conf, 
                                const CliOptions& options, BmElectionStartResponse *response, PeerId *leader_id);
butil::Status bm_election_end(const GroupId& group_id, const Configuration conf, 
                                const CliOptions& options, BmElectionEndResponse *response);
butil::Status bm_recovery_start(const GroupId& group_id, const PeerId& peer_id, const std::string& recovery_mode, 
                                const Configuration conf, const CliOptions& options, BmRecoveryStartResponse *response);
butil::Status bm_recovery_end(const GroupId& group_id, const PeerId& peer_id, const std::string& recovery_mode, 
                                const Configuration conf, const CliOptions& options, BmRecoveryEndResponse *response);
butil::Status bm_leader_transfer_start(const GroupId& group_id, const PeerId& leader_id, const PeerId& target_peer, 
                                const Configuration conf, const CliOptions& options, BmLeaderTransferStartResponse *response);

}  // namespace cli
}  //  namespace braft

#endif  //BRAFT_CLI_H
