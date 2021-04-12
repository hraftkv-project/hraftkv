#ifndef __HRAFTKV_STATE_MACHINE_HH__
#define __HRAFTKV_STATE_MACHINE_HH__

#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile

#include "hraftkv.pb.h"
#include "hraftkv.h"
#include "adapter/adapter.h"

namespace hraftkv {

class StateMachine : public braft::StateMachine {

public:
    StateMachine();
    ~StateMachine();

    bool get(const std::string &key, DataHeader* header_out, butil::IOBuf& out);

private:
    friend class Closure;
    friend class Node;
    friend class MetadataManager;

    MetadataManager* _mm;
    ::butil::Mutex _mm_mutex;
    bool is_mm_attached;

    void _attach_metadata_manager(MetadataManager *mm) {
        std::unique_lock<::butil::Mutex> lck(_mm_mutex);
        is_mm_attached = true;
        _mm = mm;
        lck.unlock();
    }

    void _detach_metadata_manager() {
        std::unique_lock<::butil::Mutex> lck(_mm_mutex);
        is_mm_attached = false;
        _mm = NULL;
        lck.unlock();
    }

    butil::atomic<int64_t> _leader_term;
    butil::atomic<bool> _is_leader;
    bool is_leader() {
        return _is_leader.load(butil::memory_order_release);
    }

    // StateMachine do not own the adapter instance
    adapter::Base* _apt;

    // implements on_apply function
    void on_apply(braft::Iterator& iter);

    // apply_metadata
    void _on_apply_metadata(
        braft::LogId& log_id, 
        butil::IOBuf& metadata, 
        const butil::IOBuf& data, 
        google::protobuf::Message* request, 
        MetadataResponse* response);

    // apply_kv_op
    void _on_apply_kv_op(
        braft::LogId& log_id, 
        braft::EntryInfo entry_info,
        butil::IOBuf& metadata, 
        const butil::IOBuf& data, 
        google::protobuf::Message* request, 
        Response* response);

    inline void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        _is_leader.store(true, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }

    inline void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        _is_leader.store(false, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    inline void on_shutdown() {
        LOG(INFO) << "This node is down";
    }

    inline void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met raft error " << e;
    }

    inline void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }

    inline void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        _is_leader.store(false, butil::memory_order_release);
        LOG(INFO) << "Node stops following " << ctx;
    }
    
    inline void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        _is_leader.store(false, butil::memory_order_release);
        LOG(INFO) << "Node start following " << ctx;
    }
};

}

#endif
