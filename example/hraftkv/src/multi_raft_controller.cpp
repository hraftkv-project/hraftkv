#include <sstream>                  // istringstream, stringstream

#include "braft/raft.h"             // braft::Status

#include "metadata.pb.h"            // GroupTable
#include "multi_raft_controller.h"

namespace hraftkv {

/**
 * MultiRaftPriorityProposer
 */
MultiRaftPriorityProposer::MultiRaftPriorityProposer(transfer_mutex_t& mutex, int timeout_ms) :
    _mutex(mutex),
    _timer_triggered(false) {
    CHECK_EQ(0, _priority_timer.init(timeout_ms));
}

void MultiRaftPriorityProposer::supply_append_entries_latency(const int64_t latency) {
    if (!_manager->is_dynamic_priority_enabled()) {
        return;
    }
    if (_priority_timer.is_timer_triggered()) {
        return;
    }
    auto itr = _manager->latency_priority_map().lower_bound(latency);
    if (itr == _manager->latency_priority_map().end()) {
        return;
    }
    int64_t new_priority = itr->second;
    if (new_priority == _manager->priority()) {
        return;
    }
    braft::NodeStatus status;
    get_node_status(&status);
    if (status.state != braft::STATE_LEADER && status.state != braft::STATE_FOLLOWER) {
        return;
    }
    if (_mutex.try_lock()) {
        LOG(INFO) << "MultiRaftPriorityProposer:: matched priority: " 
            << itr->second << " latency: " << latency;
        set_priority(new_priority);
    } else {
        _priority_timer.start();
        return;
    }
}

int MultiRaftContoller::get_idx_from_conf_str(
    const std::string& conf_str,
    const std::string& node_str
) {
    std::istringstream iss(conf_str);
    std::string token;
    int idx = 0;
    while(std::getline(iss, token, ',')) {
        if (token == node_str) {
            break;
        }
        idx++;
    }
    return idx;
}

MultiRaftPriorityProposer* MultiRaftContoller::create_priority_proposer(GroupId group_id) {
    MultiRaftPriorityProposer* proposer = new MultiRaftPriorityProposer(_transfer_mutex, PROPOSER_TIMEOUT);
    _proposer_map.insert(std::pair<GroupId, MultiRaftPriorityProposer*>(group_id, proposer));
    return proposer;
}

void MultiRaftContoller::destory_priority_proposer(GroupId group_id) {
    auto itr = _proposer_map.find(group_id);
    if (itr != _proposer_map.end()) {
        MultiRaftPriorityProposer* proposer = itr->second;
        _proposer_map.erase(group_id);
        delete proposer;
    }
}

};