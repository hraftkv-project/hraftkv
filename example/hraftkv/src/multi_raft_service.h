#ifndef __HRAFTKV_MULTI_RAFT_SERVICE_HH__
#define __HRAFTKV_MULTI_RAFT_SERVICE_HH__

#include <map>

#include "hraftkv.h"
#include "hraftkv.pb.h"
#include "node.h"

#include "multi_raft_controller.h"
#include "metadata_manager.h"

#include <braft/micro-benchmark/benchmark.h>

namespace hraftkv {

struct NodeResources {
    StateMachine* fsm;
    Node* node;
};

struct MultiRaftServiceOptions {
    MetadataManager* mm;
    MultiRaftContoller* controller;
    std::map<braft::GroupId, NodeResources>* id_node_map;
};

class MultiRaftService : public Service {
private:
    // Note that the service do not own _mm
    MetadataManager* _mm;
    // Note that the service do not own _controller
    MultiRaftContoller* _controller;

    // Note that the service do not own _id_node_map
    // it should be deleted by server.cpp
    std::map<braft::GroupId, NodeResources>* _id_node_map;

    Node* _get_node_by_gid(const braft::GroupId& gid) {
        Node* n = NULL;
        try {
            n = _id_node_map->at(gid).node;
        } catch (std::out_of_range& err) {
            LOG(ERROR) << "Unable to find Node of GroupId=" << gid;
        }
        return n;
    }

public:
    MultiRaftService(MultiRaftServiceOptions options) :
        _mm(options.mm),
        _controller(options.controller),
        _id_node_map(options.id_node_map) {}

    ~MultiRaftService() {}

    void session_id(google::protobuf::RpcController* controller,
            const SessionIdRequest* request,
            MetadataResponse* response,
            google::protobuf::Closure* done);

    void get(google::protobuf::RpcController* controller,
            const GetRequest* request,
            Response* response,
            google::protobuf::Closure* done);

    void put(google::protobuf::RpcController* controller,
            const PutRequest* request,
            Response* response,
            google::protobuf::Closure* done);

    void del(google::protobuf::RpcController* controller,
            const DeleteRequest* request,
            Response* response,
            google::protobuf::Closure* done);

    void print_bm(::google::protobuf::RpcController* controller,
                const PrintBmRequest* request,
                Response* response,
                ::google::protobuf::Closure* done);

};

}


#endif