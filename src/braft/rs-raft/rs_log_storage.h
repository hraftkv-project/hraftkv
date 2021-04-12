#ifndef RS_RAFT_LOG_STORAGE_H
#define RS_RAFT_LOG_STORAGE_H

#include <vector>
#include <map>
#include <butil/memory/ref_counted.h>
#include <butil/atomicops.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include "braft/log_entry.h"
#include "braft/storage.h"
#include "braft/util.h"

#include "rs_segment.h"

namespace braft {

/**
 * RsSegmentLogStorage is basically the same as SegmentLogStorage,
 * except it replaces the Segment to RsSegment
 */

class RsSegmentLogStorage : public SegmentLogStorage {
public:
    typedef std::map<int64_t, scoped_refptr<RsSegment>> RsSegmentMap;

    explicit RsSegmentLogStorage(const std::string& path, bool enable_sync = true)
        : SegmentLogStorage(path, enable_sync)
    {} 

    RsSegmentLogStorage()
        : SegmentLogStorage()
    {}

    virtual ~RsSegmentLogStorage() {}

    static RsSegmentLogStorage* create(const std::string& uri);

    // get logentry by index
    virtual LogEntry* get_entry(const int64_t index);

    // get logentry's term by index
    virtual int64_t get_term(const int64_t index);

    // append entry to log
    virtual int append_entry(const LogEntry* entry);

    // append entries to log, return success append number
    virtual int append_entries(const std::vector<LogEntry*>& entries);

    // delete logs from storage's head, [1, first_index_kept) will be discarded
    virtual int truncate_prefix(const int64_t first_index_kept);

    // delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
    virtual int truncate_suffix(const int64_t last_index_kept);

    virtual int reset(const int64_t next_log_index);

    virtual LogStorage* new_instance(const std::string& uri) const;

    RsSegmentMap& segments() {
        return _segments;
    }

    virtual void list_files(std::vector<std::string>* seg_files);

    virtual void sync();
protected:
    scoped_refptr<RsSegment> _open_rs_sgement(); // to differientate from open_segment()
    int list_segments(bool is_empty);
    int load_segments(ConfigurationManager* configuration_manager);
    int get_segment(int64_t log_index, scoped_refptr<RsSegment>* ptr);
    void pop_segments(
            int64_t first_index_kept, 
            std::vector<scoped_refptr<RsSegment> >* poped);
    void pop_segments_from_back(
            const int64_t first_index_kept,
            std::vector<scoped_refptr<RsSegment> >* popped,
            scoped_refptr<RsSegment>* last_segment);

    RsSegmentMap _segments;
    scoped_refptr<RsSegment> _open_segment;

    // // copy the gflags to field so that subclass can access them
    // int32_t _raft_max_segment_size;
    // bool _raft_sync_segments;
};

}

#endif