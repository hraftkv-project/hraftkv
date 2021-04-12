#ifndef __HRAFTKV_MULTI_RAFT_CONTROLLER_HH__
#define __HRAFTKV_MULTI_RAFT_CONTROLLER_HH__

#include "braft/util.h"
#include "braft/priority.h"         // PriorityProposer
#include "bthread/mutex.h"

#include "hraftkv.h"
#include "hraftkv.pb.h"             // Various Request RPC
#include "metadata.pb.h"            // GroupTable

#define PROPOSER_TIMEOUT 10000

namespace hraftkv {

typedef std::string GroupId;
typedef ::butil::Mutex transfer_mutex_t;

class ChangePriorityTimer : public braft::RepeatedTimerTask {
public:
    ChangePriorityTimer() : _timer_triggered(false) {}
    // Invoked everytime when it reaches the timeout
    virtual void run() {
        _timer_triggered = false;
    }
    virtual void start() {
        _timer_triggered = true;
        braft::RepeatedTimerTask::start();
    }
    virtual void stop() {
        _timer_triggered = false;
        braft::RepeatedTimerTask::stop();
    }
    // Invoked when the timer is finally destroyed
    virtual void on_destroy() {};

    bool is_timer_triggered() const {
        return _timer_triggered;
    }
private:
    bool _timer_triggered;
};

class MultiRaftPriorityProposer : public braft::PriorityProposer {
public:
    MultiRaftPriorityProposer(transfer_mutex_t& mutex, int timeout_ms);
    virtual ~MultiRaftPriorityProposer() {
        _priority_timer.destroy();
    }
    virtual void supply_append_entries_latency(const int64_t latency);
private:
    transfer_mutex_t& _mutex;
    bool _timer_triggered;
    ChangePriorityTimer _priority_timer;
};

class MultiRaftContoller {
public:
    ~MultiRaftContoller() {}

    // Note that ownership of GroupTable is transferred to GroupTablecontroller
    static MultiRaftContoller* create_controller_by_table(GroupTable& g_tbl) {
        MultiRaftContoller* controller = new MultiRaftContoller();
        // Parse the table to map later, now just store it
        controller->_g_tbl.Swap(&g_tbl);
        return controller;
    }

    const GroupTable& get_group_table() {
        return _g_tbl;
    }

    static int parse_str_to_group_table(
        GroupTable& g_tbl, 
        const std::string& gid_prefix, 
        const std::string& str
    );

    static int get_idx_from_conf_str(
        const std::string& conf_tr,
        const std::string& node_str
    );

    MultiRaftPriorityProposer* create_priority_proposer(GroupId group_id);
    void destory_priority_proposer(GroupId group_id);

private:
    MultiRaftContoller() {}
    GroupTable _g_tbl;

    // Priority related
    transfer_mutex_t _transfer_mutex;

    std::map<GroupId, MultiRaftPriorityProposer*> _proposer_map;
};

}

#endif