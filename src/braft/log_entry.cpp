// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#include "braft/log.h"
#include "braft/log_entry.h"
#include "braft/local_storage.pb.h"

namespace braft {

bvar::Adder<int64_t> g_nentries("raft_num_log_entries");

LogEntry::LogEntry(): type(ENTRY_TYPE_UNKNOWN), peers(NULL), old_peers(NULL),
    // RS-Raft additional fields
    k(0), fragment_index(0), fragment_len(0), valid_data_len(0) {
    g_nentries << 1;
}

LogEntry::~LogEntry() {
    g_nentries << -1;
    delete peers;
    delete old_peers;
}

// RS-Raft
int LogEntry::get_fragment(butil::IOBuf* buf, int index) {
    if (type == ENTRY_TYPE_RS_COMPLETE) {
        // Use IOBufBytesIterator to select bytes that 
        // belongs to the required fragment
        butil::IOBufBytesIterator iterator(data);
        // Calculate the offset of fragment
        size_t offset = index * fragment_len;
        // Move the iterator to the start byte of the fragment
        iterator.forward(offset);
        // Add reference of the select range to buf
        iterator.append_and_forward(buf, fragment_len);
        // LOG(INFO) << "[RS] Get fragment_index="<<index<<" of LogEntry id="
        //     <<id<<" at offset="<<offset << " and size="<<buf->length();

        // DEBUG: Compare the checksum
        // This checksum won't match now, because we append metadata to buf first
        // unsigned char* byte_array = new unsigned char[fragment_len];
        // data.copy_to(byte_array, fragment_len, fragment_len * index);
        // uint32_t byte_array_cs = get_checksum(CHECKSUM_CRC32, (const char*) byte_array, fragment_len);
        // uint32_t data_cs = get_checksum(CHECKSUM_CRC32, *buf);
        // if (byte_array_cs == data_cs) {
        //     LOG(INFO) << "[RS] Matched checksum: byte_array_cs="
        //         <<byte_array_cs<<", data_cs="<<data_cs;
        // } else {
        //     LOG(INFO) << "[RS] Unmatched checksum: byte_array_cs="
        //         <<byte_array_cs<<", data_cs="<<data_cs;
        // }

        return 0;
    } else if (type == ENTRY_TYPE_RS_FRAGMENT) {
        LOG(ERROR) << "[RS] Cannot invoke get_fragment to LogEntry id=" 
            << id << " that has type=ENTRY_TYPE_RS_FRAGMENT";
        return EPERM;
    } else {
        LOG(ERROR) << "[RS] Cannot invoke get_fragment to LogEntry " 
            << id << " that has type which does not belongs to RS";
        return EPERM;
    }
}

butil::Status parse_configuration_meta(const butil::IOBuf& data, LogEntry* entry) {
    butil::Status status;
    ConfigurationPBMeta meta;
    butil::IOBufAsZeroCopyInputStream wrapper(data);
    if (!meta.ParseFromZeroCopyStream(&wrapper)) {
        status.set_error(EINVAL, "Fail to parse ConfigurationPBMeta");
        return status;
    }
    entry->peers = new std::vector<PeerId>;
    for (int j = 0; j < meta.peers_size(); ++j) {
        entry->peers->push_back(PeerId(meta.peers(j)));
    }
    if (meta.old_peers_size() > 0) {
        entry->old_peers = new std::vector<PeerId>;
        for (int i = 0; i < meta.old_peers_size(); i++) {
            entry->old_peers->push_back(PeerId(meta.old_peers(i)));
        }
    }
    return status;    
}

butil::Status serialize_configuration_meta(const LogEntry* entry, butil::IOBuf& data) {
    butil::Status status;
    ConfigurationPBMeta meta;
    for (size_t i = 0; i < entry->peers->size(); ++i) {
        meta.add_peers((*(entry->peers))[i].to_string());
    }
    if (entry->old_peers) {
        for (size_t i = 0; i < entry->old_peers->size(); ++i) {
            meta.add_old_peers((*(entry->old_peers))[i].to_string());
        }
    }
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!meta.SerializeToZeroCopyStream(&wrapper)) {
        status.set_error(EINVAL, "Fail to serialize ConfigurationPBMeta");
    }
    return status;
}

}
