#ifndef UNIRAFT_PRIORITY_H
#define UNIRAFT_PRIORITY_H

#include <iostream>
#include <map>

#include "braft/raft.h"
#include "braft/log_manager.h"
#include "braft/errno.pb.h"
#include "braft/time.h"
#include "braft/repeated_timer_task.h"

namespace braft {
class PriorityProposer;
class PriorityManager;
class PriorityManagerStatus;

class EwmaFunc {
public:
    EwmaFunc();
    void add_value(double new_value);
    void init(std::string func_name, double alpha, bool use_init = false, double init_value = 0.0);
    
    double value() const {
        return _value;
    }

    double average() const {
        return _average;
    }

    friend std::ostream& operator<<(std::ostream& os, const EwmaFunc& func) {
        os << func.value();
        return os;
    }

private:
    std::string _func_name;
    double _alpha;
    bool _has_init;
    double _value;
    double _average; // a non-ewma average value for reference
};

class EwmaTracker : public RepeatedTimerTask {
public:
    EwmaTracker() {}
    int init(EwmaFunc* func, int timeout_ms);
    virtual ~EwmaTracker() {}
    virtual void run() = 0;
protected:
    virtual void on_destroy() {}
    // Note that the EwmaFunc is not owned by the EwmaTracker
    EwmaFunc* _func;
};

class AppendEntriesLatencyTracker : public EwmaTracker {
public:
    AppendEntriesLatencyTracker() {}
    int init(LogManager* log_manager, EwmaFunc* func, PriorityProposer* proposer, int timeout_ms, int64_t min_thresh);
    virtual ~AppendEntriesLatencyTracker() {}
    void run();
private:
    // Note that the LogManager is not owned by the Tracker
    LogManager* _log_manager;
    PriorityProposer* _proposer;
    int64_t _min_thresh;
};

/**
 * <HRaft>
 * Pass to Tracker class for updating priority value
 */
class PriorityProposer {
public:
    PriorityProposer() {}
    virtual ~PriorityProposer() {}
    
    void register_to_manager(PriorityManager* manager) {
        _manager = manager;
    }
    virtual void supply_append_entries_latency(const int64_t latency);

    void get_node_status(braft::NodeStatus* status);

    void set_priority(int64_t priority);

protected:
    PriorityManager* _manager;
};

struct PriorityManagerOptions {
    // Common fields
    bool enable_ple;
    bool enable_pre_vote_ple;
    bool enable_leadership_transfer;
    bool enable_candidate_level;
    bool enable_dynamic_priority;
    double append_entries_ewma_weight;
    int32_t append_entries_ewma_timeout_ms;
    int64_t append_entries_latency_thresh_us;
    std::string append_entries_latency_priority_map;
    
    // Specific fields
    int initial_priority;
    int initial_leader_priority;
    PriorityProposer* proposer;

    PriorityManagerOptions();

    void operator=(PriorityManagerOptions& other) {
        enable_ple = other.enable_ple;
        enable_pre_vote_ple = other.enable_pre_vote_ple;
        enable_leadership_transfer = other.enable_leadership_transfer;
        enable_candidate_level = other.enable_candidate_level;
        enable_dynamic_priority = other.enable_dynamic_priority;
        append_entries_ewma_weight = other.append_entries_ewma_weight;
        append_entries_ewma_timeout_ms = other.append_entries_ewma_timeout_ms;
        append_entries_latency_thresh_us = other.append_entries_latency_thresh_us;
        append_entries_latency_priority_map = other.append_entries_latency_priority_map;
        initial_priority = other.initial_priority;
        initial_leader_priority = other.initial_leader_priority;
        proposer = other.proposer;
    }
};

class PriorityManager {
public:
    PriorityManager(
        NodeImpl* node, 
        LogManager* log_manager, 
        const PriorityManagerOptions& options
    );
    ~PriorityManager();

    std::string status_str();
    
    /**
     * <HRaft>
     * Let Prioritized leader election enable flag accessible
     */
    bool is_ple_enabled() const { return _ple_enabled; }
    bool is_pre_vote_ple_enabled() const { 
        return _ple_enabled && _pre_vote_ple_enabled; 
    }
    bool is_leadership_transfer_enabled() const { 
        return _ple_enabled && _leadership_transfer_enabled; 
    }
    bool is_dynamic_priority_enabled() const {
        return _ple_enabled && _dynamic_priority_enabled;
    }

    const std::map<int64_t, int64_t>& latency_priority_map() const {
        return *_latency_priority_map;
    }

    /**
     * <HRaft>
     * A getter for the priority value of this server
     */
    int64_t priority() const { return _priority; }

    int64_t leader_priority() const { return _leader_priority; }

    /**
     * <HRaft>
     * A getter for checking whether this server is a probationary candidate
     */
    bool is_probationary();

    void update_leader_priority(int64_t leader_priority) { 
        if (leader_priority >= 0) {
            _leader_priority = leader_priority;
        }   
    }

    int64_t append_entries_latency_avgerage_value() const {
        return _append_entries_latency_ewma.average();
    }

    int64_t append_entries_latency_ewma_value() const {
        return _append_entries_latency_ewma.value();
    }

    // Invoked by NodeImpl during its initialization
    void start();
    // Invoked by NodeImple during its termination
    void shutdown();

private:
friend class PriorityProposer;
    /**
     * <HRaft>
     * A flag for recording whether Prioritized leader election
     *  is being used
     */
    bool _ple_enabled;
    bool _pre_vote_ple_enabled;
    bool _leadership_transfer_enabled;

    /**
     * <HRaft>
     * Prioritized leader election - Election rules
     * _priority -> priority value: the priority value of this server
     * _leader_priority -> leader priority: the priority value of current leader
     */
    int64_t _priority;
    int64_t _leader_priority;
    bool _candidate_level_enabled;

    /**
     * <HRaft>
     * Dynamic server priority
     */
    bool _own_proposer;
    PriorityProposer* _proposer;
    bool _dynamic_priority_enabled;
    EwmaFunc _append_entries_latency_ewma;
    AppendEntriesLatencyTracker _append_entries_latency_tracker;
    double _append_entries_ewma_weight;
    int32_t _append_entries_ewma_timeout_ms;
    int64_t _append_entries_latency_thresh_us;

    // Note that the LogManager is not owned by the PriorityManager
    NodeImpl* _node;
    LogManager* _log_manager;

    // set priority and do leadership transfer (if necessary)
    bool _set_priority(int64_t priority);

    // Latency - Priority map
    // initialize by constructor
    std::map<int64_t, int64_t>* _latency_priority_map;
};
}

#endif