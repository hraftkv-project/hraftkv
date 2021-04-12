#ifndef __HRAFTKV_METADATA_MANAGER_HH__
#define __HRAFTKV_METADATA_MANAGER_HH__

#include <braft/util.h>
#include <braft/raft.h>
#include <braft/log_entry.h>
#include "hraftkv.h"
#include "hraftkv.pb.h"

#include "node.h"
#include "metadata.pb.h"

#define TIMEOUT TimeVal(60,0) // sec

namespace hraftkv {

struct MetadataManagerArg {
    // MetadataManager do not own the following
    Node* node;
    StateMachine* fsm;
    bool is_multi_raft;
};

class MetadataManager {
public:
    MetadataManager();
    ~MetadataManager(){}
    void init(MetadataManagerArg* arg);

    /**
     * proceeds to commit
     */
    void handle_session_id_request(
        google::protobuf::RpcController* controller,
        const SessionIdRequest* request,
        MetadataResponse* response,
        const GroupTable* group_table,
        google::protobuf::Closure* done);

    void on_apply_session_id_request(
        const SessionIdRequest* request,
        MetadataResponse* response
    );

private:
    bool _is_multi_raft;
    // MetadataManager do not own the following
    Node* _node;
    StateMachine* _fsm;

    std::map<std::string, SessionId*> _session_id_map;

    // session id related
    SessionId* _get_session_id(const std::string& uuid);
    bool _put_session_id(const std::string& uuid, SessionId* session_id);
    bool _del_session_id(const std::string& uuid);
    bool _own_session_id(const SessionIdRequest* request);
};

class MetadataCommitClosure : public braft::Closure {
public:
    MetadataCommitClosure(Node* node, 
                const google::protobuf::Message* request, 
                MetadataResponse* response, 
                google::protobuf::Closure* done) {
        _node = node;
        _request = request;
        _response = response;
        _done = done;
    }
    ~MetadataCommitClosure() {}

    inline const google::protobuf::Message* request() const { return _request; }
    inline MetadataResponse* response() const { return _response; }
    void Run() {
        // Auto delete this after Run()
        std::unique_ptr<Closure> self_guard(this);
        // Repsond this RPC.
        brpc::ClosureGuard done_guard(_done);
        if (status().ok()) {
            return;
        }
        // Try redirect if this request failed.
        _node->redirect(_response);
    }

protected:
    Node* _node;
    const google::protobuf::Message* _request;
    MetadataResponse* _response; 
    google::protobuf::Closure* _done;
};

};

#endif