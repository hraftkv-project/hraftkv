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

#include "braft/cli.h"

#include <brpc/channel.h>          // brpc::Channel
#include <brpc/controller.h>       // brpc::Controller
#include "braft/cli.pb.h"                // CliService_Stub
#include "braft/util.h"

namespace braft {
namespace cli {

static butil::Status get_leader(const GroupId& group_id, const Configuration& conf,
                        PeerId* leader_id) {
    if (conf.empty()) {
        return butil::Status(EINVAL, "Empty group configuration");
    }
    // Construct a brpc naming service to access all the nodes in this group
    butil::Status st(-1, "Fail to get leader of group %s", group_id.c_str());
    leader_id->reset();
    for (Configuration::const_iterator
            iter = conf.begin(); iter != conf.end(); ++iter) {
        brpc::Channel channel;
        if (channel.Init(iter->addr, NULL) != 0) {
            return butil::Status(-1, "Fail to init channel to %s",
                                     iter->to_string().c_str());
        }
        CliService_Stub stub(&channel);
        GetLeaderRequest request;
        GetLeaderResponse response;
        brpc::Controller cntl;
        request.set_group_id(group_id);
        request.set_peer_id(iter->to_string());
        stub.get_leader(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            if (st.ok()) {
                st.set_error(cntl.ErrorCode(), "[%s] %s",
                            butil::endpoint2str(cntl.remote_side()).c_str(),
                            cntl.ErrorText().c_str());
            } else {
                std::string saved_et = st.error_str();
                st.set_error(cntl.ErrorCode(), "%s, [%s] %s",  saved_et.c_str(),
                            butil::endpoint2str(cntl.remote_side()).c_str(),
                            cntl.ErrorText().c_str());
                }
            continue;
        }
        leader_id->parse(response.leader_id());
    }
    if (leader_id->is_empty()) {
        return st;
    }
    return butil::Status::OK();
}

butil::Status add_peer(const GroupId& group_id, const Configuration& conf,
                       const PeerId& peer_id, const CliOptions& options) {
    PeerId leader_id;
    butil::Status st = get_leader(group_id, conf, &leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);
    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }
    AddPeerRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());
    request.set_peer_id(peer_id.to_string());
    AddPeerResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService_Stub stub(&channel);
    stub.add_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.old_peers_size(); ++i) {
        old_conf.add_peer(response.old_peers(i));
    }
    Configuration new_conf;
    for (int i = 0; i < response.new_peers_size(); ++i) {
        new_conf.add_peer(response.new_peers(i));
    }
    LOG(INFO) << "Configuration of replication group `" << group_id
              << "' changed from " << old_conf
              << " to " << new_conf;
    return butil::Status::OK();
}

butil::Status remove_peer(const GroupId& group_id, const Configuration& conf,
                         const PeerId& peer_id, const CliOptions& options) {
    PeerId leader_id;
    butil::Status st = get_leader(group_id, conf, &leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);
    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }
    RemovePeerRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());
    request.set_peer_id(peer_id.to_string());
    RemovePeerResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService_Stub stub(&channel);
    stub.remove_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.old_peers_size(); ++i) {
        old_conf.add_peer(response.old_peers(i));
    }
    Configuration new_conf;
    for (int i = 0; i < response.new_peers_size(); ++i) {
        new_conf.add_peer(response.new_peers(i));
    }
    LOG(INFO) << "Configuration of replication group `" << group_id
              << "' changed from " << old_conf
              << " to " << new_conf;
    return butil::Status::OK();
}

butil::Status reset_peer(const GroupId& group_id, const PeerId& peer_id,
                         const Configuration& new_conf,
                         const CliOptions& options) {
    if (new_conf.empty()) {
        return butil::Status(EINVAL, "new_conf is empty");
    }
    brpc::Channel channel;
    if (channel.Init(peer_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                peer_id.to_string().c_str());
    }
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    ResetPeerRequest request;
    request.set_group_id(group_id);
    request.set_peer_id(peer_id.to_string());
    for (Configuration::const_iterator
            iter = new_conf.begin(); iter != new_conf.end(); ++iter) {
        request.add_new_peers(iter->to_string());
    }
    ResetPeerResponse response;
    CliService_Stub stub(&channel);
    stub.reset_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status snapshot(const GroupId& group_id, const PeerId& peer_id,
                      const CliOptions& options, SnapshotResponse *response) {
    brpc::Channel channel;
    if (channel.Init(peer_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                peer_id.to_string().c_str());
    }
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    SnapshotRequest request;
    request.set_group_id(group_id);
    request.set_peer_id(peer_id.to_string());
    // SnapshotResponse response;
    CliService_Stub stub(&channel);
    // stub.snapshot(&cntl, &request, &response, NULL);
    stub.snapshot(&cntl, &request, response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status change_peers(const GroupId& group_id, const Configuration& conf,
                           const Configuration& new_peers,
                           const CliOptions& options) {
    PeerId leader_id;
    butil::Status st = get_leader(group_id, conf, &leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);
    LOG(INFO) << "conf=" << conf << " leader=" << leader_id
              << " new_peers=" << new_peers;
    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }

    ChangePeersRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());
    for (Configuration::const_iterator
            iter = new_peers.begin(); iter != new_peers.end(); ++iter) {
        request.add_new_peers(iter->to_string());
    }
    ChangePeersResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService_Stub stub(&channel);
    stub.change_peers(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.old_peers_size(); ++i) {
        old_conf.add_peer(response.old_peers(i));
    }
    Configuration new_conf;
    for (int i = 0; i < response.new_peers_size(); ++i) {
        new_conf.add_peer(response.new_peers(i));
    }
    LOG(INFO) << "Configuration of replication group `" << group_id
              << "' changed from " << old_conf
              << " to " << new_conf;
    return butil::Status::OK();
}

butil::Status transfer_leader(const GroupId& group_id, const Configuration& conf,
                              const PeerId& peer, const CliOptions& options) {
    PeerId leader_id;
    butil::Status st = get_leader(group_id, conf, &leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);
    if (leader_id == peer) {
        LOG(INFO) << "peer " << peer << " is already the leader";
        return butil::Status::OK();
    }
    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }
    TransferLeaderRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());
    if (!peer.is_empty()) {
        request.set_peer_id(peer.to_string());
    }
    TransferLeaderResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService_Stub stub(&channel);
    stub.transfer_leader(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status get_all_priority(const GroupId& group_id, const Configuration conf, 
                                const CliOptions& options, GetAllPriorityResponse *response, 
                                PeerId *leader_id) {
    butil::Status st = get_leader(group_id, conf, leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);

    brpc::Channel channel;
    if (channel.Init(leader_id->addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id->to_string().c_str());
    }

    GetAllPriorityRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id->to_string());

    // BmElectionStartResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService_Stub stub(&channel);
    stub.get_all_priority(&cntl, &request, response, NULL);

    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status get_leader_latency(const GroupId& group_id, const Configuration conf, 
                                const CliOptions& options, GetLeaderLatencyResponse *response, 
                                PeerId *leader_id) {
    butil::Status st = get_leader(group_id, conf, leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);

    brpc::Channel channel;
    if (channel.Init(leader_id->addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id->to_string().c_str());
    }

    GetLeaderLatencyRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id->to_string());

    // BmElectionStartResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService_Stub stub(&channel);
    stub.get_leader_latency(&cntl, &request, response, NULL);

    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

// benchmark
butil::Status bm_election_start(const GroupId& group_id, const Configuration conf, 
                                const CliOptions& options, BmElectionStartResponse *response, 
                                PeerId *leader_id) {
    butil::Status st = get_leader(group_id, conf, leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);

    brpc::Channel channel;
    if (channel.Init(leader_id->addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id->to_string().c_str());
    }

    BmElectionStartRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id->to_string());

    // BmElectionStartResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService_Stub stub(&channel);
    stub.bm_election_start(&cntl, &request, response, NULL);

    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status bm_election_end(const GroupId& group_id, const Configuration conf,
                              const CliOptions& options, BmElectionEndResponse *response) {
    PeerId leader_id;
    butil::Status st = get_leader(group_id, conf, &leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);

    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }

    BmElectionEndRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());

    // BmElectionEndResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService_Stub stub(&channel);
    stub.bm_election_end(&cntl, &request, response, NULL);

    // give client the new leader id
    response->set_leader_id(leader_id.to_string());

    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status snapshot_on_leader(const GroupId& group_id, const Configuration conf, 
                                const CliOptions& options, SnapshotResponse *response) {
    PeerId leader_id;
    butil::Status st = get_leader(group_id, conf, &leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);

    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    SnapshotRequest request;
    request.set_group_id(group_id);
    request.set_peer_id(leader_id.to_string());
    // SnapshotResponse response;
    CliService_Stub stub(&channel);
    // stub.snapshot(&cntl, &request, &response, NULL);
    stub.snapshot(&cntl, &request, response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status bm_recovery_start(const GroupId& group_id, const PeerId& peer_id, const std::string& recovery_mode, 
                                const Configuration conf, const CliOptions& options, BmRecoveryStartResponse *response) {
    PeerId leader_id;
    butil::Status st = get_leader(group_id, conf, &leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);

    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }

    BmRecoveryStartRequest request;
    request.set_mode(recovery_mode);
    request.set_peer_id(peer_id.to_string());

    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService_Stub stub(&channel);
    stub.bm_recovery_start(&cntl, &request, response, NULL);

    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status bm_recovery_end(const GroupId& group_id, const PeerId& peer_id, const std::string& recovery_mode, 
                                const Configuration conf, const CliOptions& options, BmRecoveryEndResponse *response) {
    PeerId leader_id;
    butil::Status st = get_leader(group_id, conf, &leader_id);
    BRAFT_RETURN_IF(!st.ok(), st);

    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }

    BmRecoveryEndRequest request;
    request.set_mode(recovery_mode);
    request.set_peer_id(peer_id.to_string());

    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService_Stub stub(&channel);
    stub.bm_recovery_end(&cntl, &request, response, NULL);

    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status bm_leader_transfer_start(const GroupId& group_id, const PeerId& leader_id, const PeerId& target_peer, 
                                const Configuration conf, const CliOptions& options, BmLeaderTransferStartResponse *response) {
    // PeerId leader_id;
    // butil::Status st = get_leader(group_id, conf, &leader_id);
    // BRAFT_RETURN_IF(!st.ok(), st);

    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }

    BmLeaderTransferStartRequest request;
    request.set_peer_id(target_peer.to_string());

    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService_Stub stub(&channel);
    stub.bm_leader_transfer_start(&cntl, &request, response, NULL);

    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

}  // namespace cli
}  //  namespace braft
