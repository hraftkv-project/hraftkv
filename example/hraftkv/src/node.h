#ifndef __HRAFTKV_NODE_HH__
#define __HRAFTKV_NODE_HH__

#include "braft/raft.h"                  // braft::Node braft::StateMachine
#include "braft/util.h"                  // braft::AsyncClosureGuard
#include "braft/protobuf_file.h"         // braft::ProtoBufFile
#include "braft/priority.h"              // PriorityManagerOptions

#include "hraftkv.h"
#include "hraftkv.pb.h"
#include "metadata.pb.h"
#include "state_machine.h"
#include "closure.h"

// class braft::PriorityManagerOptions;

namespace hraftkv {

struct NodeArguments {
    braft::GroupId group_id;
    butil::EndPoint addr; 
    int replication_index;
    std::string conf_str;
    StateMachine *fsm;
    braft::PriorityManagerOptions priority_manager_options;
    int election_timeout_ms;
};

class Node : public braft::Node {
    friend class Closure;
    friend class MetadataCommitClosure;

public:

    Node(NodeArguments* args);
    ~Node() {}

    // function for starting node
    int start();

    const braft::NodeOptions& get_node_options() const {
        return _node_options;
    }

    /**
     * Operation that does not need to be committed
     */
    void get(const GetRequest* request, 
                Response* response,
                google::protobuf::Closure* done) {
                    return _execute(OP_GET, request, response, done);
                }

    void print_bm(const PrintBmRequest* request,
                Response* response,
                google::protobuf::Closure* done) {
                    return _execute(OP_PRINT_BM, request, response, done);
                }

    /**
     * Operation that needs to be committed
     */

    void commit_metadata(MetadataType type,
                const google::protobuf::Message* request,
                MetadataResponse* response,
                google::protobuf::Closure* done) {
                    return _commit_metadata(type, request, response, done);
                }

    void put(const PutRequest* request, 
                Response* response,
                google::protobuf::Closure* done) {
                    return _commit(OP_PUT, request, response, done);
                }

    void del(const DeleteRequest* request, 
                Response* response,
                google::protobuf::Closure* done) {
                    return _commit(OP_DEL, request, response, done);
                }

    // Shut this node down.
    void st() {
        this->shutdown(NULL);
    }

    // Blocking this thread until the node is eventually down.
    void j() {
        this->join();
    }

    void attach_metadata_manager(MetadataManager *mm);
    MetadataManager* detach_metadata_manager();

    void redirect(Response* response);
    void redirect(MetadataResponse* response);

private:
    std::string _group_id;
    braft::NodeOptions _node_options;
    StateMachine *_fsm;

    std::string _init_conf_str;
    braft::PriorityManagerOptions _pmo;
    int _election_timeout_ms;

    ::butil::Mutex _mm_mutex;
    MetadataManager *_mm;
    bool is_mm_attached;

    void _get(
        const GetRequest* request,
        Response* response,
        google::protobuf::Closure* done);
    
    /**
     * Operation that does not need to be committed
     */
    void _execute(KVStoreOpType type,
            const google::protobuf::Message* request,
            Response* response,
            google::protobuf::Closure* done);
            
    /**
     * Operation that needs to be committed
     */
    void _commit_metadata(MetadataType type,
        const google::protobuf::Message* request,
        MetadataResponse* response,
        google::protobuf::Closure* done);
    void _commit(KVStoreOpType type, 
        const google::protobuf::Message* request, 
        Response* response, 
        google::protobuf::Closure* done);
    
};

}; // namespace hraftkv 

#endif
