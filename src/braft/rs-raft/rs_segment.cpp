
#include "braft/log.h"

#include <gflags/gflags.h>
#include <butil/files/dir_reader_posix.h>            // butil::DirReaderPosix
#include <butil/file_util.h>                         // butil::CreateDirectory
#include <butil/string_printf.h>                     // butil::string_appendf
#include <butil/time.h>
#include <butil/raw_pack.h>                          // butil::RawPacker
#include <butil/fd_utility.h>                        // butil::make_close_on_exec
#include <brpc/reloadable_flags.h>             // 

#include "braft/local_storage.pb.h"
#include "braft/log_entry.h"
#include "braft/protobuf_file.h"
#include "braft/util.h"
#include "braft/fsync.h"

#include "rs_segment.h"
// #include "rs_code.h"                                // print_bytes for DEBUG

using namespace braft;

using ::butil::RawPacker;
using ::butil::RawUnpacker;

const static size_t RS_ENTRY_HEADER_SIZE = 40;

struct RsSegment::EntryHeader {
    int64_t term;
    int type;
    int checksum_type;
    int k;
    int fragment_index;
    uint32_t data_len;
    uint32_t data_checksum;

    // Additional data for RS-Raft
    uint32_t metadata_len;
    uint32_t metadata_checksum;
    uint32_t fragment_len;
    uint32_t valid_data_len;
};

std::ostream& operator<<(std::ostream& os, const RsSegment::EntryHeader& h) {
    os << "{term=" << h.term << ", type=" << h.type << ", data_len="
       << h.data_len << ", checksum_type=" << h.checksum_type << ", k=" << h.k << ", fragment_index=" << h.fragment_index
       << ", data_checksum=" << h.data_checksum << ", metedata_len=" << h.metadata_len
       << ", metadata_checksum=" << h.metadata_checksum << ", fragment_len=" << h.fragment_len
       << ", valid_data_len=" << h.valid_data_len << '}';
    return os;
}

int ftruncate_uninterrupted(int fd, off_t length) {
    int rc = 0;
    do {
        rc = ftruncate(fd, length);
    } while (rc == -1 && errno == EINTR);
    return rc;
}

int RsSegment::load(ConfigurationManager* configuration_manager) {
    int ret = 0;

    std::string path(_path);
    // create fd
    if (_is_open) {
        butil::string_appendf(&path, "/" BRAFT_SEGMENT_OPEN_PATTERN, _first_index);
    } else {
        butil::string_appendf(&path, "/" BRAFT_SEGMENT_CLOSED_PATTERN, 
                             _first_index, _last_index.load());
    }
    _fd = ::open(path.c_str(), O_RDWR);
    if (_fd < 0) {
        LOG(ERROR) << "Fail to open " << path << ", " << berror();
        return -1;
    }
    butil::make_close_on_exec(_fd);

    // get file size
    struct stat st_buf;
    if (fstat(_fd, &st_buf) != 0) {
        LOG(ERROR) << "Fail to get the stat of " << path << ", " << berror();
        ::close(_fd);
        _fd = -1;
        return -1;
    }

    // load entry index
    int64_t file_size = st_buf.st_size;
    int64_t entry_off = 0;
    int64_t actual_last_index = _first_index - 1;
    for (int64_t i = _first_index; entry_off < file_size; i++) {
        RsSegment::EntryHeader header;
        const int rc = _load_entry(entry_off, &header, NULL, NULL, RS_ENTRY_HEADER_SIZE);
        if (rc > 0) {
            // The last log was not completely written, which should be truncated
            break;
        }
        if (rc < 0) {
            ret = rc;
            break;
        }
        // rc == 0
        const int64_t skip_len = RS_ENTRY_HEADER_SIZE + header.metadata_len + header.data_len;
        if (entry_off + skip_len > file_size) {
            // The last log was not completely written and it should be
            // truncated
            break;
        }
        if (header.type == ENTRY_TYPE_CONFIGURATION) {
            butil::IOBuf data;
            // Header will be parsed again but it's fine as configuration
            // changing is rare
            if (_load_entry(entry_off, NULL, NULL, &data, skip_len) != 0) {
                break;
            }
            scoped_refptr<LogEntry> entry = new LogEntry();
            entry->id.index = i;
            entry->id.term = header.term;
            butil::Status status = parse_configuration_meta(data, entry);
            if (status.ok()) {
                ConfigurationEntry conf_entry(*entry);
                configuration_manager->add(conf_entry); 
            } else {
                LOG(ERROR) << "fail to parse configuration meta, path: " << _path
                    << " entry_off " << entry_off;
                ret = -1;
                break;
            }
        }
        _offset_and_term.push_back(std::make_pair(entry_off, header.term));
        ++actual_last_index;
        entry_off += skip_len;
    }

    const int64_t last_index = _last_index.load(butil::memory_order_relaxed);
    if (ret == 0 && !_is_open) {
        if (actual_last_index < last_index) {
            LOG(ERROR) << "data lost in a full segment, path: " << _path
                << " first_index: " << _first_index << " expect_last_index: "
                << last_index << " actual_last_index: " << actual_last_index;
            ret = -1;
        } else if (actual_last_index > last_index) {
            // FIXME(zhengpengfei): should we ignore garbage entries silently
            LOG(ERROR) << "found garbage in a full segment, path: " << _path
                << " first_index: " << _first_index << " expect_last_index: "
                << last_index << " actual_last_index: " << actual_last_index;
            ret = -1;
        }
    }

    if (ret != 0) {
        return ret;
    }

    if (_is_open) {
        _last_index = actual_last_index;
    }

    // truncate last uncompleted entry
    if (entry_off != file_size) {
        LOG(INFO) << "truncate last uncompleted write entry, path: " << _path
            << " first_index: " << _first_index << " old_size: " << file_size << " new_size: " << entry_off;
        ret = ftruncate_uninterrupted(_fd, entry_off);
    }

    // seek to end, for opening segment
    ::lseek(_fd, entry_off, SEEK_SET);

    _bytes = entry_off;
    return ret;
}


int RsSegment::append(const LogEntry* entry) {

    if (BAIDU_UNLIKELY(!entry || !_is_open)) {
        return EINVAL;
    } else if (entry->id.index != 
                    _last_index.load(butil::memory_order_consume) + 1) {
        CHECK(false) << "entry->index=" << entry->id.index
                  << " _last_index=" << _last_index
                  << " _first_index=" << _first_index;
        return ERANGE;
    }

    if (entry->type == ENTRY_TYPE_DATA) {
        LOG(ERROR) << "Write a LogEntry with ENTRY_TYPE_DATA when RsSegmentLogStorage is being used";
        return EPERM;
    }

    butil::IOBuf metadata;
    butil::IOBuf data;
    switch (entry->type) {
        case ENTRY_TYPE_RS_REPLICATE:
        case ENTRY_TYPE_RS_COMPLETE:
        case ENTRY_TYPE_RS_FRAGMENT: {
            metadata.append(entry->metadata);
            data.append(entry->data);
            break;
        }
        case ENTRY_TYPE_NO_OP:
            break;
        case ENTRY_TYPE_CONFIGURATION: 
            {
                butil::Status status = serialize_configuration_meta(entry, data);
                if (!status.ok()) {
                    LOG(ERROR) << "Fail to serialize ConfigurationPBMeta, path: " 
                            << _path;
                    return -1; 
                }
            }
            break;
        default:
            LOG(FATAL) << "unknow entry type: " << entry->type
                    << ", path: " << _path;
            return -1;
    }

    CHECK_LE(data.length(), 1ul << 56ul);
    char header_buf[RS_ENTRY_HEADER_SIZE];

    // Construct the meta_field row
    // | entry-type (8bits) | checksum_type (8bits) | k (8bits) | fragment_index (8bits) |
    const uint32_t meta_field = ((uint32_t) entry->type << 24) | ((uint32_t) _checksum_type << 16) | ((uint32_t) entry->k << 8) | ((uint32_t) entry->fragment_index);

    // Pack all data into header
    // Format of Header, all fields are in network order
    // | -------------------------------- term (64bits) -------------------------------  |
    // | entry-type (8bits) | checksum_type (8bits) | k (8bits) | fragment_index (8bits) |
    // | ------------------ data len (32bits) | data_checksum (32bits) ----------------- |
    // | -------------- metadata len (32bits) | metadata checksum (32bits) ------------- |
    // | -------------- fragment len (32bits) | valid_data_len (32bits) ---------------- |
    // | ---------------------------header checksum (32bits) --------------------------- |
    RawPacker packer(header_buf);
    packer.pack64(entry->id.term)
          .pack32(meta_field)
          .pack32((uint32_t) data.length())
          .pack32(get_checksum(_checksum_type, data))
          .pack32((uint32_t) metadata.length())
          .pack32(get_checksum(_checksum_type, metadata))
          .pack32(entry->fragment_len)
          .pack32(entry->valid_data_len);
    packer.pack32(get_checksum(
                  _checksum_type, header_buf, RS_ENTRY_HEADER_SIZE - 4));                 
    butil::IOBuf header;
    header.append(header_buf, RS_ENTRY_HEADER_SIZE);

    // DEBUG
    // std::stringstream ss;
    // print_bytes(ss, "[RS-DEBUG] header save content: ", (const unsigned char*) header_buf, RS_ENTRY_HEADER_SIZE, true);
    // LOG(INFO) << "metadata.length()=" << metadata.length() << "get_checksum(_checksum_type, metadata)=" << get_checksum(_checksum_type, metadata) << "_checksum_type: " << _checksum_type << ss.str();

    const size_t to_write = header.length() + metadata.length() + data.length();
    butil::IOBuf* pieces[3] = { &header, &metadata, &data };
    size_t start = 0;
    ssize_t written = 0;
    while (written < (ssize_t)to_write) {
        const ssize_t n = butil::IOBuf::cut_multiple_into_file_descriptor(
                _fd, pieces + start, ARRAY_SIZE(pieces) - start);
        if (n < 0) {
            LOG(ERROR) << "Fail to write to fd=" << _fd 
                       << ", path: " << _path << berror();
            return -1;
        }
        written += n;
        for (;start < ARRAY_SIZE(pieces) && pieces[start]->empty(); ++start) {}
    }
    BAIDU_SCOPED_LOCK(_mutex);
    _offset_and_term.push_back(std::make_pair(_bytes, entry->id.term));
    _last_index.fetch_add(1, butil::memory_order_relaxed);
    _bytes += to_write;

    return 0;
}

LogEntry* RsSegment::get(const int64_t index) const {

    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        return NULL;
    }

    bool ok = true;
    LogEntry* entry = NULL;
    do {
        ConfigurationPBMeta configuration_meta;
        EntryHeader header;
        butil::IOBuf data;
        butil::IOBuf metadata;
        if (_load_entry(meta.offset, &header, &metadata, &data, 
                        meta.length) != 0) {
            ok = false;
            break;
        }
        CHECK_EQ(meta.term, header.term);
        entry = new LogEntry();
        entry->AddRef();

        switch (header.type) {
            case ENTRY_TYPE_RS_REPLICATE:
            case ENTRY_TYPE_RS_COMPLETE:
            case ENTRY_TYPE_RS_FRAGMENT: {
                entry->metadata.swap(metadata);
                entry->data.swap(data);
                break;
            }
            case ENTRY_TYPE_NO_OP:
                CHECK(data.empty()) << "Data of NO_OP must be empty";
                break;
            case ENTRY_TYPE_CONFIGURATION:
                {
                    butil::Status status = parse_configuration_meta(data, entry); 
                    if (!status.ok()) {
                        LOG(WARNING) << "Fail to parse ConfigurationPBMeta, path: "
                                    << _path;
                        ok = false;
                        break;
                    }
                }
                break;
            case ENTRY_TYPE_DATA: {
                LOG(ERROR) << "Write a LogEntry with ENTRY_TYPE_DATA when RsSegmentLogStorage is being used";
                ok = false;
                break;
            }
            default:
                CHECK(false) << "Unknown entry type, path: " << _path;
                break;
        }

        if (!ok) { 
            break;
        }
        entry->id.index = index;
        entry->id.term = header.term;
        entry->type = (EntryType)header.type;

        // Insert additional entry information of RS-Raft
        entry->k = header.k;
        entry->fragment_index = header.fragment_index;
        entry->fragment_len = header.fragment_len;
        entry->valid_data_len = header.valid_data_len;
    } while (0);

    if (!ok && entry != NULL) {
        entry->Release();
        entry = NULL;
    }
    return entry;
}

int RsSegment::_load_entry(off_t offset, RsSegment::EntryHeader* head, butil::IOBuf* metadata
                        , butil::IOBuf* data, size_t size_hint) const {
    butil::IOPortal buf;
    size_t to_read = std::max(size_hint, RS_ENTRY_HEADER_SIZE);
    const ssize_t n = file_pread(&buf, _fd, offset, to_read);
    if (n != (ssize_t)to_read) {
        return n < 0 ? -1 : 1;
    }

    // Extract the entry header
    // Format of Header, all fields are in network order
    // | -------------------------------- term (64bits) -------------------------------  |
    // | entry-type (8bits) | checksum_type (8bits) | k (8bits) | fragment_index (8bits) |
    // | ------------------ data len (32bits) | data_checksum (32bits) ----------------- |
    // | -------------- metadata len (32bits) | metadata checksum (32bits) ------------- |
    // | -------------- fragment len (32bits) | valid_data_len (32bits) ---------------- |
    // | ---------------------------header checksum (32bits) --------------------------- |
    char header_buf[RS_ENTRY_HEADER_SIZE];
    const char *p = (const char *)buf.fetch(header_buf, RS_ENTRY_HEADER_SIZE);
    // original header
    int64_t term = 0;
    uint32_t meta_field;
    uint32_t data_len = 0;
    uint32_t data_checksum = 0;
    // RS-Raft header
    uint32_t metadata_len = 0;
    uint32_t metadata_checksum = 0;
    uint32_t fragment_len = 0;
    uint32_t valid_data_len = 0;
    uint32_t header_checksum = 0;


    RawUnpacker(p).unpack64((uint64_t&)term)
                  .unpack32(meta_field)
                  .unpack32(data_len)
                  .unpack32(data_checksum)
                  .unpack32(metadata_len)
                  .unpack32(metadata_checksum)
                  .unpack32(fragment_len)
                  .unpack32(valid_data_len)
                  .unpack32(header_checksum);

    // Unpack the header and check the entry type
    EntryHeader tmp;
    tmp.term = term;
    tmp.type = meta_field >> 24;
    tmp.checksum_type = (meta_field << 8) >> 24;
    tmp.k = (meta_field << 16) >> 24;
    tmp.fragment_index = (meta_field << 24) >> 24;
    tmp.data_len = data_len;
    tmp.data_checksum = data_checksum;
    tmp.metadata_len = metadata_len;
    tmp.metadata_checksum = metadata_checksum;
    tmp.fragment_len = fragment_len;
    tmp.valid_data_len = valid_data_len;

    // DEBUG
    // std::stringstream ss;
    // print_bytes(ss, "[RS-DEBUG] header load content: ", (const unsigned char *) p, RS_ENTRY_HEADER_SIZE, true);
    // LOG(INFO) << "tmp.checksum_type: " << tmp.checksum_type << ss.str() << " header: " << tmp;
    
    // Veryify header checksum
    if (!verify_checksum(tmp.checksum_type, 
                        p, RS_ENTRY_HEADER_SIZE - 4, header_checksum)) {
        LOG(ERROR) << "Found corrupted header at offset=" << offset
                   << ", header=" << tmp << ", path: " << _path;
        return -1;
    }
    if (head != NULL) {
        *head = tmp;
    }

    // Read payload (metadata and data)
    size_t total_size = RS_ENTRY_HEADER_SIZE + metadata_len + data_len;
    
    if (buf.length() < total_size) {
        const size_t to_read = total_size - buf.length();
        const ssize_t n = file_pread(&buf, _fd, offset + buf.length(), to_read);
        if (n != (ssize_t)to_read) {
            return n < 0 ? -1 : 1;
        }
    } else if (buf.length() > total_size) {
        buf.pop_back(buf.length() - total_size);
    }
    CHECK_EQ(buf.length(), total_size);
    buf.pop_front(RS_ENTRY_HEADER_SIZE);

    if (metadata != NULL) {
        // load metadata part to metadta IOBuf and verify checksum
        buf.cutn(metadata, metadata_len);
        if (!verify_checksum(tmp.checksum_type, *metadata, tmp.metadata_checksum)) {
            LOG(ERROR) << "Found corrupted metadata at offset=" 
                       << offset + RS_ENTRY_HEADER_SIZE
                       << " header=" << tmp
                       << " path: " << _path;
            // TODO: abort()?
            return -1;
        }
    } else if (metadata_len > 0) {
        // If metadata == NULL, which means the caller don't want the metadata part,
        // pop it away.
        buf.pop_front(metadata_len);
    }

    if (data != NULL) {
        // load data part to data IOBuf and verify checksum
        data->swap(buf);
        if (!verify_checksum(tmp.checksum_type, *data, tmp.data_checksum)) {
            LOG(ERROR) << "Found corrupted data at offset=" 
                       << offset + RS_ENTRY_HEADER_SIZE
                       << " header=" << tmp
                       << " path: " << _path;
            // TODO: abort()?
            return -1;
        }
    }
    return 0;
}