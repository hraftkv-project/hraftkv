#include <experimental/filesystem>
#include <butil/raw_pack.h>

#include "hraftkv.h"
#include "hraftkv.pb.h"
#include "adapter/adapter.h"
#include "closure.h"

#include "metadata_manager.h"
#include "config.h"
#include "state_machine.h"
#include "braft/micro-benchmark/benchmark.h"
#include "braft/time.h"

using namespace hraftkv;

StateMachine::StateMachine() {
    adapter::Type apt_type = config::Config::get_instance().get_apt_type();
    _apt = adapter::get_adapter(apt_type);
    if (_apt == NULL) {
        LOG(ERROR) << "Cannot initialize adapter";
        exit(1);
    }
    _leader_term = -1;
    _mm = NULL;
}

StateMachine::~StateMachine() {
    // delete _apt;
}

void StateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        // This guard helps invoke iter.done()->Run() asynchronously to
        // avoid that callback blocks the StateMachine.
        braft::AsyncClosureGuard done_guard(iter.done());

        // Get log id
        braft::LogId log_id(iter.index(), iter.term());
        // Copy the metadata (as we need to modify it later)
        butil::IOBuf metadata = iter.metadata();
        // Get reference of the data
        const butil::IOBuf& data = iter.data();
        // Get the data type
        braft::EntryInfo entry_info = iter.entry_info();
       
        // Fetch the type of operation from the leading byte.
        uint8_t commit_type = COMMIT_UNKNOWN;
        metadata.cutn(&commit_type, sizeof(uint8_t));
        CommitType parse_type = static_cast<CommitType>(commit_type);

        switch (parse_type) {
            case COMMIT_METADATA: {
                MetadataCommitClosure* c = NULL;
                if (iter.done()) {
                    c = dynamic_cast<MetadataCommitClosure*>(iter.done());
                }
                // Extract the request and response from closure
                google::protobuf::Message* request = c ? const_cast<google::protobuf::Message*>(c->request()) : NULL;
                MetadataResponse* response = c ? c->response() : NULL;
                _on_apply_metadata(log_id, metadata, data, request, response);
                break;
            }

            case COMMIT_DATA: {
                Closure* c = NULL;
                if (iter.done()) {
                    c = dynamic_cast<Closure *>(iter.done());
                }
                // Extract the request and response from closure
                google::protobuf::Message* request = c ? const_cast<google::protobuf::Message*>(c->request()) : NULL;
                Response* response = c ? const_cast<Response*>(c->response()) : NULL;
                _on_apply_kv_op(log_id, entry_info, metadata, data, request, response);
                break;
            }

            default:
                LOG(ERROR) << "Received entry with log_id=" << log_id << " with unknown CommitType=" << commit_type
                        << "," << entry_info;
        }
    }
}

// apply_metadata
void StateMachine::_on_apply_metadata(
                    braft::LogId& log_id,
                    butil::IOBuf& metadata,
                    const butil::IOBuf& data, 
                    google::protobuf::Message* request, 
                    MetadataResponse* response) {
    // MetadataManager
    MetadataResponse* actual_response = response;
    /**
     * If response is NULL, which means this node is not leader.
     * We create a dummy response for the procedures after, 
     * but it will not be replied to client
     */
    if (actual_response == NULL) {
        MetadataResponse dummy_response;
        dummy_response.set_type(META_UNKNOWN); // make it to be unknown
        actual_response = &dummy_response;
    }

    uint8_t type = META_UNKNOWN; // MetadataType
    metadata.cutn(&type, sizeof(uint8_t));

    switch (type) {
        case META_SESSION_ID: {
            // deserialize the request
            SessionIdRequest *actual_request;
            if (request) {
                // This task is applied by this node, get value from this
                // closure to avoid additional parsing.
                actual_request = dynamic_cast<SessionIdRequest*>(request);
            } else {
                butil::IOBufAsZeroCopyInputStream wrapper(metadata);
                actual_request = new SessionIdRequest();
                CHECK(actual_request->ParseFromZeroCopyStream(&wrapper));
            }

            // return the request to metadata manager for further process
            std::unique_lock<::butil::Mutex> lck(_mm_mutex);
            if (is_mm_attached) {
                _mm->on_apply_session_id_request(actual_request, actual_response);
            } else {
                LOG(ERROR) << "State machine receive SessionIdRequest but MetadataManager is not attached";
            }
            lck.unlock();
        }
    }
}

bool StateMachine::get(const std::string &key, DataHeader* header_out, butil::IOBuf& out) {
    // Get the storage buf from adapter
    bool success = _apt->get(key, out);

    // If success, cut the header
    // Header structuress
    // | entry_type (8 bits) | reserved (24 bits) |
    if (success) {
        uint32_t meta_field;
        char header_buf[DATA_HEADER_SIZE];
        out.cutn(&header_buf, DATA_HEADER_SIZE);
        butil::RawUnpacker(header_buf)
                    .unpack32(meta_field);
        header_out->entry_type = meta_field >> 24;
    }

    return success;
}

// apply_kv_op
void StateMachine::_on_apply_kv_op(
                    braft::LogId& log_id,
                    braft::EntryInfo entry_info,
                    butil::IOBuf& metadata,
                    const butil::IOBuf& data, 
                    google::protobuf::Message* request, 
                    Response* response) {
    // Response* actual_response = dynamic_cast<Response*>(response);
    Response* actual_response = response;
    /**
     * If response is NULL, which means this node is not leader.
     * We create a dummy response for the procedures after, 
     * but it will not be replied to client
     */
    if (actual_response == NULL) {
        Response dummy_response;
        actual_response = &dummy_response;
    }

    uint8_t type = OP_UNKNOWN; // KVStoreOpType
    metadata.cutn(&type, sizeof(uint8_t));

    // Execute the operation according to type
    switch (type) {
        case OP_PUT: {
            // Deserialize request from metadata
            auto actual_request = dynamic_cast<PutRequest*>(request);
            if (actual_request == NULL) {
                butil::IOBufAsZeroCopyInputStream wrapper(metadata);
                actual_request = new PutRequest();
                CHECK(actual_request->ParseFromZeroCopyStream(&wrapper));
            }
            #ifdef MICRO_BM
            // Benchmark
            bm::Benchmark &bm = bm::Benchmark::getInstance();
            bm::Put *put_func = static_cast<bm::Put*>(bm.at(bm.bm_func_id(actual_request->session_id(), bm::BenchmarkType::OP_KV_PUT)));
            // TagPt (start): storage_io
            TagPt storage_io;
            storage_io.markStart();
            #endif

            // Append header to storage_buf
            // Header structure
            // | entry_type (8 bits) | reserved (24 bits) |
            butil::IOBuf storage_buf;
            const uint32_t meta_field = (entry_info.type << 24);

            char header_buf[DATA_HEADER_SIZE];
            butil::RawPacker packer(header_buf);
            packer.pack32(meta_field);
            storage_buf.append(header_buf, DATA_HEADER_SIZE);
            /**
             * A bug in IOBuf cause crashing when deleting storage_buf
             * if we append a const IOBuf data to it.
             * So we convert data to string first and append to stroage_buf
             * to exercise data copying. Fix it later.
             */
            storage_buf.append(data.to_string());

            // Insert storage_buf to adapter
            bool success = _apt->put(actual_request->key(), storage_buf);
            actual_response->set_success(success);

            // Remove the data from storage_buf, because it is located in the log, 
            // which cannot be free when deleting storage_buf at the the function lifetime
            storage_buf.pop_back(data.length());

            #ifdef MICRO_BM
            // TAGPT (end): storage_io
            storage_io.markEnd();
            if (put_func) {
                put_func->storage_io_time.Observe(storage_io.usedTime());
                // Record the key and value size
                put_func->total_key_size.Increment(actual_request->key().length());
                put_func->total_value_size.Increment(data.length());
                put_func->applied_operations.Increment();
            }
            #endif
            break;
        }
            
        case OP_DEL: {
            auto actual_request = dynamic_cast<DeleteRequest*>(request);
            if (actual_request == NULL) {
                butil::IOBufAsZeroCopyInputStream wrapper(metadata);
                actual_request = new DeleteRequest();
                CHECK(actual_request->ParseFromZeroCopyStream(&wrapper));
            }

            bool success = _apt->del(actual_request->key());
            actual_response->set_success(success);
            break;
        }

        default:
            CHECK(false) << "Unknown type=" << static_cast<int>(type);
            break;
    }
}
