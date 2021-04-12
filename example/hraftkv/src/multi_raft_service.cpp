#include "multi_raft_service.h"

namespace hraftkv {

void MultiRaftService::session_id(google::protobuf::RpcController* cntl,
            const SessionIdRequest* request,
            MetadataResponse* response,
            google::protobuf::Closure* done) {
    _mm->handle_session_id_request(
            cntl, request, response, &_controller->get_group_table(), done);
}

void MultiRaftService::get(google::protobuf::RpcController* controller,
            const GetRequest* request,
            Response* response,
            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    braft::GroupId gid = request->group_id();
    Node* node = _get_node_by_gid(gid);
    if (!node) {
        response->set_success(false);
        return;
    }
    #ifdef MICRO_BM
        bm::Benchmark &bm = bm::Benchmark::getInstance();
        bm::Get *get = static_cast<bm::Get*>(bm.at(bm.bm_func_id(request->session_id(), bm::BenchmarkType::OP_KV_GET)));
        /**
         * Currently, the Get Benchmark function will be created in Metadata Manager
         * during the creation of Session ID on leader node. 
         * However, after leadership transfer, the new leader does not have that function
         * and should be created here
         */
        if (get == NULL) {
            get = new bm::Get();
            bm.add(get, bm.bm_func_id(request->session_id(), bm::BenchmarkType::OP_KV_GET));
        }

        // insert unit
        bm::GetUnit *unit = new bm::GetUnit();
        int unitId = get->addUnit((bm::BaseBMUnit *) unit);

        // insert unitId to the request
        auto req = const_cast<GetRequest*>(request);
        req->set_unit_id(unitId);
        // TAGPT (start): overall
        unit->overall.markStart();
    #endif        
    node->get(request, response, done_guard.release()); 
}

void MultiRaftService::put(google::protobuf::RpcController* controller,
        const PutRequest* request,
        Response* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    braft::GroupId gid = request->group_id();
    Node* node = _get_node_by_gid(gid);
    if (!node) {
        response->set_success(false);
        return;
    }
    node->put(request, response, done_guard.release());
}

void MultiRaftService::del(google::protobuf::RpcController* controller,
        const DeleteRequest* request,
        Response* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    braft::GroupId gid = request->group_id();
    Node* node = _get_node_by_gid(gid);
    if (!node) {
        response->set_success(false);
        return;
    }
    node->del(request, response, done_guard.release());
}

void MultiRaftService::print_bm(::google::protobuf::RpcController* controller,
            const PrintBmRequest* request,
            Response* response,
            ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);    
    braft::GroupId gid = request->group_id();
    Node* node = _get_node_by_gid(gid);
    if (!node) {
        response->set_success(false);
        return;
    }
    node->print_bm(request, response, done_guard.release());
}

} // namespace hraftkv