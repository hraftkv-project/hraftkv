#include <string>
#include <experimental/filesystem>
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include "braft/time.h"
#include "config.h"
#include "node.h"
#include "metadata_manager.h"

using namespace std::experimental;
using namespace hraftkv;
using namespace hraftkv::config;

MetadataManager::MetadataManager() : 
    _is_multi_raft(false),
    _node(NULL),
    _fsm(NULL) {}

void MetadataManager::init(MetadataManagerArg* arg) {
    _node = arg->node;
    _fsm = arg->fsm;
    _is_multi_raft = arg->is_multi_raft;
}

void MetadataManager::handle_session_id_request(
        google::protobuf::RpcController* controller,
        const SessionIdRequest* request,
        MetadataResponse* response,
        const GroupTable* group_table,
        google::protobuf::Closure* done) {

    // run Run() automatically if early return
    brpc::ClosureGuard done_guard(done);

    /**
     * Check whether the attached node is the leader of the 
     * raft group
     * If yes: proceed
     * If no: redirect the client to leader
     */
    if (!_node->is_leader()) {
        LOG(ERROR) << "MetadataManager::handle_session_id_request: receives SessionIdRequest but not a leader";
        _node->redirect(response);
        return;
    }

    // get data from request
    SessionIdRequest::OpType operation = request->operation();
    const std::string& uuid = request->uuid();
    const std::string& req_ip = request->ip();

    // check if the session id exists
    SessionId* session_id = NULL;
    session_id = _get_session_id(uuid);

    // create and insert a SessionIdResponse to MetadataResponse
    SessionIdResponse* sid_response = new SessionIdResponse();
    response->set_type(META_SESSION_ID);
    response->set_allocated_session_id(sid_response);

    if (_is_multi_raft && (request->group_table_version() < group_table->version())) {
        CHECK(group_table) << "MetadataManager::handle_session_id_request: Multi-Raft is enabled but group_table is null";
        GroupTable* g_tbl_to_client = sid_response->mutable_group_table();
        g_tbl_to_client->CopyFrom(*group_table);
    }

    switch (operation) {
        case SessionIdRequest_OpType_CREATE: {
            if (session_id != NULL) {
                const std::string& recorded_ip = session_id->ip();
                response->set_success(req_ip == recorded_ip);
                return;
            } 
            
            // generate an uuid and commit
            boost::uuids::random_generator generator;
            std::string new_uuid;
            std::stringstream ss;
            do {
                ss.clear();
                boost::uuids::uuid uuid_raw = generator();
                ss << uuid_raw;
                new_uuid = ss.str();
            } while (_get_session_id(new_uuid) != NULL);

            // generate a lease time
            TimeVal lease;
            lease.mark();
            lease = lease + TIMEOUT;

            // pass to Node to commit
            auto req_not_const = const_cast<SessionIdRequest*>(request);
            req_not_const->set_uuid(new_uuid);
            req_not_const->set_lease_time(lease.sec());
            LOG(INFO) << "<MetadtaManager> Commit new session id with uuid: " << new_uuid;

            _node->commit_metadata(META_SESSION_ID, req_not_const, response, done_guard.release());
            break;
        }

        case SessionIdRequest_OpType_RENEW:
            if (session_id == NULL) { // failed
                response->set_success(false);
                return;
            }
            break;

        case SessionIdRequest_OpType_EXPIRE: {
            if (session_id == NULL) { // failed
                response->set_success(false);
                return;
            }
            // check if the use own the session id
            if (_own_session_id(request)) {
                LOG(INFO) << "<MetadataManager> Expire the session id with uuid: " << uuid;
                // commit the expire
                _node->commit_metadata(META_SESSION_ID, request, response, done_guard.release());
            } else {
                response->set_success(false);
                return;
            }
            break;
        }
    }
}

void MetadataManager::on_apply_session_id_request(
        const SessionIdRequest* request,
        MetadataResponse* response) {

    // get operation type
    SessionIdRequest::OpType operation = request->operation();
    // get the SessionIdResponse if it is not a dummy
    SessionIdResponse* sid_response = NULL;
    MetadataType type = static_cast<MetadataType>(response->type());
    if (type == META_SESSION_ID) {
        sid_response = response->mutable_session_id();
    }

    switch (operation) {
        case SessionIdRequest_OpType_CREATE: {
            // create SessionId instance from SessionIdRequest
            SessionId* new_id = new SessionId();
            new_id->set_ip(request->ip());
            new_id->set_lease_time(request->lease_time());
            // save the session id to store
            bool success = _put_session_id(request->uuid(), new_id);

            LOG(INFO) << "<MetadataManager> New session id - uuid: " << request->uuid();
            LOG(INFO) << "<MetadataManager> New session id - lease_time: " << request->lease_time();

#ifdef MICRO_BM
            // create benchmark function of this session id
            bm::Benchmark &bm = bm::Benchmark::getInstance();
            bm::Get* get = new bm::Get();
            if (request->has_bm_name()) {
                get->set_bm_name(request->bm_name());
            }
            bm.add(get, bm.bm_func_id(request->uuid(), bm::BenchmarkType::OP_KV_GET));
            // bm::Put* put = new bm::Put();
            bm::Put* put = bm::Put::create_function(request->uuid(), request->bm_name());
            bm.add(put, bm.bm_func_id(request->uuid(), bm::BenchmarkType::OP_KV_PUT));
#endif

            // set the response
            response->set_success(success);
            if (sid_response) {
                sid_response->set_uuid(request->uuid());
                sid_response->set_lease_time(request->lease_time());
            }
            break;
        }

        case SessionIdRequest_OpType_RENEW:
            break;

        case SessionIdRequest_OpType_EXPIRE: {
            // TODO: delete related contents of the session id
            // delete the session id
            bool success = _del_session_id(request->uuid());
            std::stringstream ss;
            ss << "<MetdataManager> Expire session id with uuid: " << request->uuid();
            ss << (success ?  " successfully" : " unsuccessfully");
            LOG(INFO) << ss.str();

#ifdef MICRO_BM
            bm::Benchmark &bm = bm::Benchmark::getInstance();
            bm::Put *put = static_cast<bm::Put*>(bm.at(bm.bm_func_id(request->uuid(), bm::BenchmarkType::OP_KV_PUT)));
            // Read the state machine size
            size_t storage_size = _fsm->_apt->get_storage_size();
            // LOG(ERROR) << "Put Expire: storage_size="<<storage_size;
            put->storage_size.Increment(storage_size);

            // Read the raft log size 
            const Raft &raft = Config::get_instance().get_raft();
            size_t total_size = 0U;
            // TODO: loop over the log directory of all Raft group
            try {
                // remove and recreate the leveldb directory
                filesystem::path log_dir(raft.data_path());
                for (filesystem::directory_entry const& entry : filesystem::directory_iterator(log_dir)) {
                    if (filesystem::is_regular_file(entry.status())) {
                        total_size += filesystem::file_size(entry.path());
                    }
                }
            } catch (filesystem::filesystem_error &ex) { 
                LOG(ERROR) << "Node::get data error: " << ex.what();
            }
            // LOG(ERROR) << "Put Expire: total_size="<<total_size;
            put->raft_log_size.Increment(total_size);
            put->finish();

            // clear benchmark functions
            bm::Get *get = static_cast<bm::Get*>(bm.at(bm.bm_func_id(request->uuid(), bm::BenchmarkType::OP_KV_GET)));
            if (get != NULL) {
                delete get;
                bm.remove(bm.bm_func_id(request->uuid(), bm::BenchmarkType::OP_KV_GET));
            }
            if (put != NULL) {
                delete put;
                bm.remove(bm.bm_func_id(request->uuid(), bm::BenchmarkType::OP_KV_PUT));
            }
#endif

            response->set_success(success);
            break;
        }
    }
}

SessionId* MetadataManager::_get_session_id(const std::string& uuid) {
    std::map<std::string, SessionId*>::iterator it = _session_id_map.find(uuid);
    if (it != _session_id_map.end()) {
       return it->second;
    }
    return NULL;
}

bool MetadataManager::_put_session_id(const std::string& uuid, SessionId* session_id) {
    _session_id_map.insert(std::pair<const std::string, SessionId*>(uuid, session_id));
    return true;
}

bool MetadataManager::_del_session_id(const std::string& uuid) {
    SessionId* id = _get_session_id(uuid);
    if (id == NULL) {
        return false;
    }
    // delete the SessionId object
    delete id;
    // erase the element from map
    _session_id_map.erase(uuid);
    return true;
}

bool MetadataManager::_own_session_id(const SessionIdRequest* request) {
    // TODO: implement the checking later
    return true;
}
