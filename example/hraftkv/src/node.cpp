#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include <brpc/callback.h>         // brpc::NewCallback

#include "braft/enum.pb.h"
#include "braft/priority.h"
#include "braft/micro-benchmark/benchmark.h"

#include "hraftkv.h"
#include "node.h"
#include "closure.h"
#include "config.h"
#include "metadata_manager.h"

using namespace hraftkv;
using namespace config;

Node::Node(
    NodeArguments* args
) : braft::Node(
    args->group_id, 
    braft::PeerId(args->addr, args->replication_index)
) {
    _group_id = args->group_id;
    _init_conf_str = args->conf_str;
    _fsm = args->fsm;
    _pmo = args->priority_manager_options;
    _mm = NULL;
    _election_timeout_ms = args->election_timeout_ms;
}

void Node::attach_metadata_manager(MetadataManager *mm) {
    std::unique_lock<::butil::Mutex> lck(_mm_mutex);
    is_mm_attached = true;
    _mm = mm;
    _fsm->_attach_metadata_manager(mm);
    lck.unlock();
}

MetadataManager* Node::detach_metadata_manager() {
    std::unique_lock<::butil::Mutex> lck(_mm_mutex);
    is_mm_attached = false;
    MetadataManager* mm = _mm;
    _mm = NULL;
    _fsm->_detach_metadata_manager();
    lck.unlock();
    return mm;
}

// function for starting node
int Node::start() {
    // init node options
    const Raft &raft = Config::get_instance().get_raft();
    if (_node_options.initial_conf.parse_from(_init_conf_str) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << _init_conf_str << '\'';
        return -1;
    }

    // _node_options.election_timeout_ms = raft.election_timeout_ms();
    _node_options.election_timeout_ms = _election_timeout_ms;
    _node_options.stepdown_timeout_ms = raft.stepdown_timeout_ms();
    _node_options.vote_timeout_ms = raft.vote_timeout_ms();
    _node_options.catchup_margin = 10000;
    _node_options.fsm = dynamic_cast<braft::StateMachine*>(_fsm);
    _node_options.node_owns_fsm = false;
    _node_options.snapshot_interval_s = raft.snapshot_interval();

    LOG(INFO) << "Snapshot_interval has been set to " << raft.snapshot_interval();

    std::string prefix = "local://" + raft.data_path() + "/" + _group_id;
    _node_options.log_uri = prefix + "/log";
    _node_options.raft_meta_uri = prefix + "/raft_meta";
    _node_options.snapshot_uri = prefix + "/snapshot";
    _node_options.disable_cli = raft.disable_cli();
    _node_options.priority_manager_options = &_pmo;

    if (this->init(_node_options) != 0) {
        LOG(ERROR) << "Fail to init raft node";
        return -1;
    }

    return 0;
}

void Node::_get(
        const GetRequest* req,
        Response* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    // Get key from GetRequest
    const std::string& key = req->key();
    // Retreive data from StateMachine
    butil::IOBuf stripe;
    DataHeader header;
    bool success = _fsm->get(key, &header, stripe);
    
#ifdef MICRO_BM
    bm::GetUnit *get = (bm::GetUnit *) bm::Benchmark::getUnit(req->session_id(), bm::BenchmarkType::OP_KV_GET, req->unit_id());
    get->performAction.markEnd();
#endif

    if (success) {
        // Check the entry type
        switch(header.entry_type) {
            case braft::ENTRY_TYPE_RS_REPLICATE: {
                response->set_value(stripe.to_string());
                break;
            }
            default: {
                LOG(ERROR) << "Unknown entry type="<<header.entry_type;
            }
        }
    }

    response->set_success(success);

#ifdef MICRO_BM
    get->overall.markEnd();
#endif
}

void Node::_execute(KVStoreOpType type,
            const google::protobuf::Message* request,
            Response* response,
            google::protobuf::Closure* done) {
    // make sure closure->Run() will be run if early return happened
    brpc::ClosureGuard done_guard(done);

    // serialize request to IOBuf
    const int64_t term = _fsm->_leader_term.load(butil::memory_order_relaxed);
    if (term < 0) {
        return redirect(response);
    }

    switch(type) {
        case OP_GET: {
            const GetRequest *req = dynamic_cast<const GetRequest*>(request);
#ifdef MICRO_BM
            // TAGPT (start): perform action
            bm::GetUnit *get = (bm::GetUnit *) bm::Benchmark::getUnit(req->session_id(), bm::BenchmarkType::OP_KV_GET, req->unit_id());
            get->performAction.markStart();
#endif
            this->_get(req, response, done_guard.release());
            break;
        }

        case OP_PRINT_BM: {
            const PrintBmRequest *req = dynamic_cast<const PrintBmRequest*>(request);
            bm::Benchmark &bm = bm::Benchmark::getInstance();
            bm::BenchmarkType type = static_cast<bm::BenchmarkType>(req->func_id());
            std::string func_id = bm.bm_func_id(req->uuid(), type);
            bm::BaseBMFunc *func = bm.at(func_id);
            
            if (func == NULL) {
                LOG(ERROR) << "funcId: " << func_id << " is not found, cannot print benchmark stats";
                response->set_success(false);
                return;
            }

            /**
             * Parse BMFunc
             * Currently assume funcId is KVStoreOpType
             * and use it to confirm BMFunc type
             */
            switch(type) {
                case bm::BenchmarkType::OP_KV_GET: {
                    unsigned long int keySizeTotal;
                    unsigned long int valueSizeTotal;

                    bm::Get *get_func = static_cast<bm::Get*>(func);
                    std::map<std::string, double> *tvMap = get_func->calcStats(keySizeTotal, valueSizeTotal);
                    std::stringstream res_ss = get_func->printStats(keySizeTotal, valueSizeTotal, tvMap);

                    // set response
                    response->set_success(true);
                    // add result to response
                    response->set_value(res_ss.str());

                    break;
                }

                case bm::BenchmarkType::OP_KV_PUT: {
                    // set response
                    response->set_success(true);
                    // add result to response
                    response->set_value("");
                    break;
                }
                default:
                    LOG(ERROR) << "Benchmark do not support BMFunc type: " << func_id;
            }
            break;
        }

        default: {
            LOG(ERROR) << "Unknown execution type: " << type;
        }
    }
}

void Node::_commit_metadata(MetadataType type,
        const google::protobuf::Message* raw_request,
        MetadataResponse* response,
        google::protobuf::Closure* done) {

    brpc::ClosureGuard done_guard(done);
    // LOG(INFO) << "apply_metadata, pass 1";
    // serialize request to IOBuf
    const int64_t term = _fsm->_leader_term.load(butil::memory_order_relaxed);
    if (term < 0) {
        return redirect(response);
        return;
    }

    switch(type) {
        case META_SESSION_ID: {
            const SessionIdRequest *request = dynamic_cast<const SessionIdRequest*>(raw_request);
            butil::IOBuf log;
            log.push_back((uint8_t) COMMIT_METADATA); // let statemachine know it is a metadata
            log.push_back((uint8_t) type); // metadata type
            butil::IOBufAsZeroCopyOutputStream wrapper(&log);
            if (!request->SerializeToZeroCopyStream(&wrapper)) {
                LOG(ERROR) << "Fail to serialize request";
                response->set_success(false);
                return;
            }
            // Apply this log as a braft::Task
            braft::RsTask task;
            task.metadata = &log;
            // This callback would be iovoked when the task actually executed or fail
            // task.done = new Closure(this, request, response, done_guard.release());
            task.done = new MetadataCommitClosure(this, request, response, done_guard.release());
            if (Config::get_instance().get_raft().check_term()) {
                // ABA problem can be avoid if expected_term is set
                task.expected_term = term;
            }
            // Set enable_coding flag to false, 
            // as session id don't have data
            braft::Node::rs_apply(task, false);
            break;
        }

        default: {
            LOG(ERROR) << "Unknown metadata type found: " << type;
        }
    }
}

void Node::_commit(KVStoreOpType type,
            const google::protobuf::Message* request,
            Response* response,
            google::protobuf::Closure* done) {

    // make sure closure->Run() will be run if early return happened
    brpc::ClosureGuard done_guard(done);

    // serialize request to IOBuf
    const int64_t term = _fsm->_leader_term.load(butil::memory_order_relaxed);
    if (term < 0) {
        return redirect(response);
    }

    switch(type) {        
        case OP_PUT: {
            const PutRequest *req = dynamic_cast<const PutRequest*>(request);

            // Serialize data
            butil::IOBuf data;
            data.append(req->value());
            // Clear the value in PutRequest before serializing PutRequest to metadata
            // as we don't want to include it in the metadata
            PutRequest *mutable_req = const_cast<PutRequest*>(req);
            mutable_req->clear_value();
            mutable_req->set_value(""); // set an empty string to serialization
            // Serialize metadata
            butil::IOBuf metadata;
            metadata.push_back((uint8_t) COMMIT_DATA); // let statemachine know it is a data
            metadata.push_back((uint8_t) type); // let statemachine know about the key-value operation type
            butil::IOBufAsZeroCopyOutputStream metadata_os(&metadata);
            // Release the value in PutRequest
            if (!req->SerializeToZeroCopyStream(&metadata_os)) {
                LOG(ERROR) << "Fail to serialize request to metadata";
                response->set_success(false);
                return;
            }
            // Put them into task
            braft::RsTask task;
            task.metadata = &metadata;
            task.data = &data;
            task.done = new Closure(this, request, response, done_guard.release());
#ifdef MICRO_BM
            // Pass the benchmark identifier to the closure
            task.done->set_bm_identifier(bm::Benchmark::bm_func_id(req->session_id(), bm::BenchmarkType::OP_KV_PUT));
#endif
            if (Config::get_instance().get_raft().check_term()) {
                // ABA problem can be avoid if expected_term is set
                task.expected_term = term;
            }
            braft::Node::rs_apply(task, false);
            break;
        }
        case OP_DEL: {
            butil::IOBuf log;
            log.push_back((uint8_t) COMMIT_DATA); // let statemachine know it is a data
            log.push_back((uint8_t) type); // let statemachine know about the key-value operation type
            butil::IOBufAsZeroCopyOutputStream wrapper(&log);
            if (!request->SerializeToZeroCopyStream(&wrapper)) {
                LOG(ERROR) << "Fail to serialize request";
                response->set_success(false);
                return;
            }
            braft::RsTask task;
            task.metadata = &log;
            // This callback would be iovoked when the task actually executed or fail
            task.done = new Closure(this, request, response, done_guard.release());
            if (Config::get_instance().get_raft().check_term()) {
                // ABA problem can be avoid if expected_term is set
                task.expected_term = term;
            }
            // Set enable_coding flag to false, 
            // as DelRequest don't have data
            braft::Node::rs_apply(task, false);
            break;
        }

        default: {
            LOG(ERROR) << "Unknown operation: " << type;
        } 
    }
}

void Node::redirect(Response* response) {
    response->set_success(false);
    braft::PeerId leader = this->leader_id();
    if (!leader.is_empty()) {
        response->set_redirect(leader.to_string());
    }
}

void Node::redirect(MetadataResponse* response) {
    response->set_success(false);
    braft::PeerId leader = this->leader_id();
    if (!leader.is_empty()) {
        response->set_redirect(leader.to_string());
    }
}
