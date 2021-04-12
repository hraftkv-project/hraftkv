#include <limits>
#include <gflags/gflags.h>                       // DEFINE_int32
#include <brpc/reloadable_flags.h>               // BRPC_VALIDATE_GFLAG

#include "braft/log_manager.h"
#include "braft/priority.h"

using namespace braft;

/**
 * <HRaft>
 * Switch to enable or disable prioritized leader election
 */
DEFINE_bool(raft_enable_ple, false, "Enable prioritized leader election");
BRPC_VALIDATE_GFLAG(raft_enable_ple, ::brpc::PassValidate);

DEFINE_bool(raft_enable_pre_vote_ple, false, "Enable prioritized leader election at PreVote stage");
BRPC_VALIDATE_GFLAG(raft_enable_pre_vote_ple, ::brpc::PassValidate);

DEFINE_bool(raft_enable_leadership_transfer, false, "Enable leadership transfer");
BRPC_VALIDATE_GFLAG(raft_enable_leadership_transfer, ::brpc::PassValidate);

DEFINE_bool(raft_enable_candidate_level, true, "Enable candidate levels");
BRPC_VALIDATE_GFLAG(raft_enable_candidate_level, ::brpc::PassValidate);

/**
 * <HRaft>
 * Dynamic server priority
 */
DEFINE_bool(raft_enable_dynamic_priority, true, "Enable Dynamic server priority");
BRPC_VALIDATE_GFLAG(raft_enable_dynamic_priority, ::brpc::PassValidate);
DEFINE_double(raft_append_entries_ewma_weight, 0.2, "Ewma weight of append entries latency");
BRPC_VALIDATE_GFLAG(raft_append_entries_ewma_weight, ::brpc::PassValidate);
DEFINE_int32(raft_append_entries_ewma_timeout_ms, 1000, "Time period that EwmaFunc update its value");
BRPC_VALIDATE_GFLAG(raft_append_entries_ewma_timeout_ms, brpc::PositiveInteger);
DEFINE_int64(raft_append_entries_latency_thresh_us, 1000, "Threshold of append entries to trigger ewma update");
BRPC_VALIDATE_GFLAG(raft_append_entries_latency_thresh_us, brpc::PositiveInteger);

#define MAX_STR "MAX"

DEFINE_string(raft_append_entries_latency_priority_map, 
    "MAX:1,40000:2,20000:3,10000:4,5000:5",
    "Map for append entries latency and priority value, format: <upper_boundary>:<priority>");

#include "braft/node.h"

bool _is_override(std::string flag_name) {
    return !gflags::GetCommandLineFlagInfoOrDie(flag_name.c_str()).is_default;
}

/**
 * EwmaFunc
 */
EwmaFunc::EwmaFunc() : _has_init(false) {}

void EwmaFunc::add_value(double new_value) {
    _value = _has_init ? (_alpha * (new_value - _value) + _value) : new_value;
    _average = _has_init ? (0.5 * (new_value - _average) + _average) : new_value;
    _has_init = true;
}

void EwmaFunc::init(std::string func_name, double alpha, bool use_init, double init_value) {
    _func_name = func_name;
    _alpha = alpha;
    _has_init = use_init;
    _value = init_value;
    _average = init_value;
}

/**
 * EwmaTracker
 */
int EwmaTracker::init(EwmaFunc* func, int timeout_ms) {
    BRAFT_RETURN_IF(RepeatedTimerTask::init(timeout_ms) != 0, -1);
    _func = func;
    return 0;
}

/**
 * AppendEntriesLatencyTracker
 */
int AppendEntriesLatencyTracker::init( 
        LogManager* log_manager, EwmaFunc* func, PriorityProposer* proposer, int timeout_ms, int64_t min_thresh) {
    BRAFT_RETURN_IF(EwmaTracker::init(func, timeout_ms) != 0, -1);
    _log_manager = log_manager;
    _proposer = proposer;
    _min_thresh = min_thresh;
    return 0;
}

void AppendEntriesLatencyTracker::run() {
    bvar::LatencyRecorder& latency = _log_manager->get_storage_append_entries_latency();
    if (latency.latency() >= _min_thresh) {
        _func->add_value(latency.latency());
    }
    // bvar::IntRecorder& written_size = _log_manager->get_storage_append_entries_written_size();
    // LOG(INFO) << "append_entries_latency - normal: " << latency.latency() << ", ewma:" 
    //             << _func->value() << ", written_size: " << written_size.average();
    _proposer->supply_append_entries_latency(_func->value());
}

/**
 * PriorityProposer
 */
void PriorityProposer::supply_append_entries_latency(const int64_t latency) {
    if (_manager->_dynamic_priority_enabled) {
        // check the state of this node
        NodeStatus status;
        _manager->_node->get_status(&status);
        // allowing change priority value when node is either leader or follower
        if (status.state == STATE_LEADER || status.state == STATE_FOLLOWER) {
            auto itr = _manager->_latency_priority_map->lower_bound(latency);
            if (itr != _manager->_latency_priority_map->end()) {
                LOG(INFO) << "PriorityProposer:: matched priority: " << itr->second << " latency: " << latency;
                int64_t new_priority = itr->second;
                _manager->_set_priority(new_priority);
            }
        }
    }
}

void PriorityProposer::get_node_status(NodeStatus* status) {
    _manager->_node->get_status(status);
}

void PriorityProposer::set_priority(int64_t priority) {
    _manager->_set_priority(priority);
}

/**
 * PriorityManager
 */
PriorityManagerOptions::PriorityManagerOptions() :
    enable_ple(FLAGS_raft_enable_ple),
    enable_pre_vote_ple(FLAGS_raft_enable_pre_vote_ple),
    enable_leadership_transfer(FLAGS_raft_enable_leadership_transfer),
    enable_candidate_level(FLAGS_raft_enable_candidate_level),
    enable_dynamic_priority(FLAGS_raft_enable_dynamic_priority),
    append_entries_ewma_weight(FLAGS_raft_append_entries_ewma_weight),
    append_entries_ewma_timeout_ms(FLAGS_raft_append_entries_ewma_timeout_ms),
    append_entries_latency_thresh_us(FLAGS_raft_append_entries_latency_thresh_us),
    append_entries_latency_priority_map(FLAGS_raft_append_entries_latency_priority_map),
    initial_priority(0),
    initial_leader_priority(0),
    proposer(NULL) {}

PriorityManager::PriorityManager(
    NodeImpl* node, 
    LogManager* log_manager, 
    const PriorityManagerOptions& options
):  _node(node),
    _log_manager(log_manager) {

    _ple_enabled = _is_override("raft_enable_ple") ? FLAGS_raft_enable_ple : options.enable_ple;
    _pre_vote_ple_enabled = _is_override("raft_enable_pre_vote_ple") ? FLAGS_raft_enable_pre_vote_ple : options.enable_pre_vote_ple;
    _leadership_transfer_enabled = _is_override("raft_enable_leadership_transfer") ? FLAGS_raft_enable_leadership_transfer : options.enable_leadership_transfer;
    _candidate_level_enabled = _is_override("raft_enable_candidate_level") ? FLAGS_raft_enable_candidate_level : options.enable_candidate_level;
    _dynamic_priority_enabled = _is_override("raft_enable_dynamic_priority") ? FLAGS_raft_enable_dynamic_priority : options.enable_dynamic_priority;
    _priority = options.initial_priority;
    _leader_priority = options.initial_leader_priority;

    /**
     * PriorityProposer
     * options.proposer != null: upper layer app pass its own proposer 
     *                           to override the native one
     * options.proposer == null: create one itself
     */
    _own_proposer = options.proposer == NULL;
    _proposer = (options.proposer == NULL) ? 
        new PriorityProposer() : options.proposer;
    _proposer->register_to_manager(this);
    _append_entries_ewma_weight = _is_override("raft_append_entries_ewma_weight") ? FLAGS_raft_append_entries_ewma_weight : options.append_entries_ewma_weight;
    _append_entries_ewma_timeout_ms = _is_override("raft_append_entries_ewma_timeout_ms") ? FLAGS_raft_append_entries_ewma_timeout_ms : options.append_entries_ewma_timeout_ms;
    _append_entries_latency_thresh_us = _is_override("raft_append_entries_latency_thresh_us") ? FLAGS_raft_append_entries_latency_thresh_us : options.append_entries_latency_thresh_us;

    // create the latency - priority map
    _latency_priority_map = new std::map<int64_t, int64_t>();

    // convert input flag string to latency - priority map
    std::stringstream map_ss(
        _is_override("raft_append_entries_latency_priority_map") ? 
            FLAGS_raft_append_entries_latency_priority_map :
            options.append_entries_latency_priority_map
    );
    while (map_ss.good()) {
        std::string level;
        getline(map_ss, level, ',');

        std::string upper_boundary = level.substr(0, level.find(':'));
        int64_t upper_boundary_val = upper_boundary == MAX_STR ? std::numeric_limits<int64_t>::max() : std::stoi(upper_boundary);
        std::string priority = level.substr(level.find(':') + 1, level.length());
        int64_t priority_val = std::stoi(priority);

        LOG(INFO) << "[PLE] level: " << level << "upper_boundary_val: " 
            << upper_boundary_val << " priority_val: " << priority_val;
        std::pair<int64_t, int64_t> level_pair(upper_boundary_val, priority_val);
        _latency_priority_map->insert(level_pair);
    }

}

PriorityManager::~PriorityManager() {
    delete _latency_priority_map;
    if (_own_proposer) {
        delete _proposer;
    }
}

bool PriorityManager::is_probationary() { 
    return _candidate_level_enabled ? (_priority < _leader_priority) : false; 
}

void PriorityManager::start() {
    // if (_dynamic_priority_enabled) {
        _append_entries_latency_ewma.init("append_entries_latency", _append_entries_ewma_weight);
        _append_entries_latency_tracker.init(
            _log_manager, 
            &_append_entries_latency_ewma, 
            _proposer,
            _append_entries_ewma_timeout_ms,
            _append_entries_latency_thresh_us
        );
        _append_entries_latency_tracker.start();
    // }
}

void PriorityManager::shutdown() {
    // if (_dynamic_priority_enabled) {
        _append_entries_latency_tracker.stop();
        _append_entries_latency_tracker.destroy();
    // }
}

std::string PriorityManager::status_str() {
    std::stringstream os;
    os << "\n========= Priority Manager =========\n"
        << "[switch]\n"
        << " - ple_enabled: " << is_ple_enabled() << "\n"
        << " - pre_vote_ple_enabled: " << is_pre_vote_ple_enabled() << "\n"
        << " - leadership_transfer_enabled: " << is_leadership_transfer_enabled() << "\n"
        << " - dynamic_priority_enabled: " << _dynamic_priority_enabled << "\n"
        << " - candidate_level_enabled: " << _candidate_level_enabled << "\n"
        << "[priority]\n"
        << " - priority: " << priority() << "\n"
        << " - leader_priority: " << leader_priority() << "\n"
        << "[dynamic priority]\n"
        << " - append_entries_ewma_weight: " << _append_entries_ewma_weight << "\n"
        << " - append_entries_ewma_timeout_ms: " << _append_entries_ewma_timeout_ms << "\n"
        << " - append_entries_latency_thresh_us: " << _append_entries_latency_thresh_us  << "\n"
        << "========= Priority Manager =========";
    return os.str();
}

bool PriorityManager::_set_priority(int64_t priority) {
    bool shouldUpdate = _priority != priority;
    if (shouldUpdate) {
        // update priority if not identical
        _priority = priority;
        // check of leadership transfer
        _node->transfer_leadership_to(ANY_PEER, false);
    }
    return shouldUpdate;
}