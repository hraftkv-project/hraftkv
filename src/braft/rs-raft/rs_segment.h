#ifndef RS_RAFT_LOG_H
#define RS_RAFT_LOG_H

#include <vector>
#include <map>
#include <butil/memory/ref_counted.h>
#include <butil/atomicops.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include "braft/log_entry.h"
#include "braft/storage.h"
#include "braft/util.h"

#include "braft/log.h"

#define BRAFT_SEGMENT_OPEN_PATTERN "log_inprogress_%020" PRId64
#define BRAFT_SEGMENT_CLOSED_PATTERN "log_%020" PRId64 "_%020" PRId64
#define BRAFT_SEGMENT_META_FILE  "log_meta"

namespace braft {

class BAIDU_CACHELINE_ALIGNMENT RsSegment : public Segment {
public:
    RsSegment(const std::string& path, const int64_t first_index, int checksum_type)
        : Segment(path, first_index, checksum_type)
    {}

    RsSegment(const std::string& path, const int64_t first_index, const int64_t last_index,
            int checksum_type)
        : Segment(path, first_index, last_index, checksum_type)
    {}

    // // load open or closed segment
    // // open fd, load index, truncate uncompleted entry
    virtual int load(ConfigurationManager* configuration_manager);

    // serialize entry, and append to open segment
    virtual int append(const LogEntry* entry);

    // get entry by index
    virtual LogEntry* get(const int64_t index) const;

private:
friend class butil::RefCountedThreadSafe<RsSegment>;
    ~RsSegment() {
        if (_fd >= 0) {
            ::close(_fd);
            _fd = -1;
        }
    }

    int _load_entry(off_t offset, RsSegment::EntryHeader* head, butil::IOBuf* metadata
                        , butil::IOBuf* data, size_t size_hint) const;
};

};

#endif