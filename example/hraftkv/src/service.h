#ifndef __HRAFTKV_SERVICE_HH__
#define __HRAFTKV_SERVICE_HH__

#include "hraftkv.h"
#include "hraftkv.pb.h"
#include "node.h"

#include "metadata_manager.h"

#include <braft/micro-benchmark/benchmark.h>

namespace hraftkv {

class ServiceImpl : public Service {
public:

    explicit ServiceImpl(Node* node, MetadataManager* mm) {
        _node = node;
        _mm = mm;
    }
    ~ServiceImpl() {}

    void session_id(google::protobuf::RpcController* controller,
            const SessionIdRequest* request,
            MetadataResponse* response,
            google::protobuf::Closure* done) {
        return _mm->handle_session_id_request(controller, request, response, NULL, done);
    }

    void get(google::protobuf::RpcController* controller,
            const GetRequest* request,
            Response* response,
            google::protobuf::Closure* done) {
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
#endif
        // insert unitId to the request
        auto req = const_cast<GetRequest*>(request);
#ifdef MICRO_BM
        req->set_unit_id(unitId);
        // TAGPT (start): overall
        unit->overall.markStart();
#endif        
        return _node->get(req, response, done);           
    }

    void put(google::protobuf::RpcController* controller,
            const PutRequest* request,
            Response* response,
            google::protobuf::Closure* done) {
        // insert unitId to the request
        auto req = const_cast<PutRequest*>(request);
        return _node->put(req, response, done);           
    }

    void del(google::protobuf::RpcController* controller,
            const DeleteRequest* request,
            Response* response,
            google::protobuf::Closure* done) {
        return _node->del(request, response, done);   
    }

    void print_bm(::google::protobuf::RpcController* controller,
                const PrintBmRequest* request,
                Response* response,
                ::google::protobuf::Closure* done) {
        return _node->print_bm(request, response, done);
    }

private:
    Node* _node;
    MetadataManager* _mm;
};

}

#endif
