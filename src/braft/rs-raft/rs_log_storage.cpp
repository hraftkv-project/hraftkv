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
#include "braft/log_entry.h"

#include "rs_log_storage.h"

using namespace braft;

/**
 * RsSegmentLogStorage
 */

RsSegmentLogStorage* RsSegmentLogStorage::create(const std::string& uri) {
    butil::StringPiece copied_uri(uri);
    std::string parameter;
    // The function parse_uri will help trim out the protocol prefix
    // Node that RsSegmentLogStorage only receive local:// prefix
    butil::StringPiece protocol = parse_uri(&copied_uri, &parameter);
    if (protocol.empty() || protocol.as_string() != "local") {
        LOG(ERROR) << "Invalid log storage uri=`" << uri << '\'';
        return NULL;
    }
    LOG(INFO) << "RsSegmentLogStorage::create";
    return new RsSegmentLogStorage(copied_uri.as_string());
}

LogEntry* RsSegmentLogStorage::get_entry(const int64_t index) {
    scoped_refptr<RsSegment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return NULL;
    }
    return ptr->get(index);
}

int64_t RsSegmentLogStorage::get_term(const int64_t index) {
    scoped_refptr<RsSegment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return 0;
    }
    return ptr->get_term(index);
}

int RsSegmentLogStorage::append_entry(const LogEntry* entry) {
    scoped_refptr<RsSegment> segment = _open_rs_sgement();
    if (NULL == segment) {
        return EIO;
    }
    int ret = segment->append(entry);
    if (ret != 0 && ret != EEXIST) {
        return ret;
    }
    if (EEXIST == ret && entry->id.term != get_term(entry->id.index)) {
        return EINVAL;
    }
    _last_log_index.fetch_add(1, butil::memory_order_release);

    return segment->sync(_enable_sync);
}

int RsSegmentLogStorage::append_entries(const std::vector<LogEntry*>& entries) {
    if (entries.empty()) {
        return 0;
    }
    if (_last_log_index.load(butil::memory_order_relaxed) + 1
            != entries.front()->id.index) {
        LOG(FATAL) << "There's gap between appending entries and _last_log_index"
                   << " path: " << _path;
        return -1;
    }
    scoped_refptr<RsSegment> last_segment = NULL;
    for (size_t i = 0; i < entries.size(); i++) {
        LogEntry* entry = entries[i];

        scoped_refptr<RsSegment> segment = _open_rs_sgement();
        if (NULL == segment) {
            return i;
        }
        int ret = segment->append(entry);
        if (0 != ret) {
            return i;
        }
        _last_log_index.fetch_add(1, butil::memory_order_release);
        last_segment = segment;
    }
    
    last_segment->sync(_enable_sync);
    return entries.size();
}

int RsSegmentLogStorage::truncate_prefix(const int64_t first_index_kept) {
    // segment files
    if (_first_log_index.load(butil::memory_order_acquire) >= first_index_kept) {
      BRAFT_VLOG << "Nothing is going to happen since _first_log_index=" 
                     << _first_log_index.load(butil::memory_order_relaxed)
                     << " >= first_index_kept="
                     << first_index_kept;
        return 0;
    }
    // NOTE: truncate_prefix is not important, as it has nothing to do with 
    // consensus. We try to save meta on the disk first to make sure even if
    // the deleting fails or the process crashes (which is unlikely to happen).
    // The new process would see the latest `first_log_index'
    if (save_meta(first_index_kept) != 0) { // NOTE
        PLOG(ERROR) << "Fail to save meta, path: " << _path;
        return -1;
    }
    std::vector<scoped_refptr<RsSegment> > popped;
    pop_segments(first_index_kept, &popped);
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i] = NULL;
    }
    return 0;
}

int RsSegmentLogStorage::truncate_suffix(const int64_t last_index_kept) {
    // segment files
    std::vector<scoped_refptr<RsSegment> > popped;
    scoped_refptr<RsSegment> last_segment;
    pop_segments_from_back(last_index_kept, &popped, &last_segment);
    bool truncate_last_segment = false;
    int ret = -1;

    if (last_segment) {
        if (_first_log_index.load(butil::memory_order_relaxed) <=
            _last_log_index.load(butil::memory_order_relaxed)) {
            truncate_last_segment = true;
        } else {
            // trucate_prefix() and truncate_suffix() to discard entire logs
            BAIDU_SCOPED_LOCK(_mutex);
            popped.push_back(last_segment);
            _segments.erase(last_segment->first_index());
            if (_open_segment) {
                CHECK(_open_segment.get() == last_segment.get());
                _open_segment = NULL;
            }
        }
    }

    // The truncate suffix order is crucial to satisfy log matching property of raft
    // log must be truncated from back to front.
    for (size_t i = 0; i < popped.size(); ++i) {
        ret = popped[i]->unlink();
        if (ret != 0) {
            return ret;
        }
        popped[i] = NULL;
    }
    if (truncate_last_segment) {
        bool closed = !last_segment->is_open();
        ret = last_segment->truncate(last_index_kept);
        if (ret == 0 && closed && last_segment->is_open()) {
            BAIDU_SCOPED_LOCK(_mutex);
            CHECK(!_open_segment);
            _open_segment.swap(last_segment);
        }
    }

    return ret;
}

int RsSegmentLogStorage::reset(const int64_t next_log_index) {
    if (next_log_index <= 0) {
        LOG(ERROR) << "Invalid next_log_index=" << next_log_index
                   << " path: " << _path;
        return EINVAL;
    }
    std::vector<scoped_refptr<RsSegment> > popped;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    popped.reserve(_segments.size());
    for (RsSegmentMap::const_iterator 
            it = _segments.begin(); it != _segments.end(); ++it) {
        popped.push_back(it->second);
    }
    _segments.clear();
    if (_open_segment) {
        popped.push_back(_open_segment);
        _open_segment = NULL;
    }
    _first_log_index.store(next_log_index, butil::memory_order_relaxed);
    _last_log_index.store(next_log_index - 1, butil::memory_order_relaxed);
    lck.unlock();
    // NOTE: see the comments in truncate_prefix
    if (save_meta(next_log_index) != 0) {
        PLOG(ERROR) << "Fail to save meta, path: " << _path;
        return -1;
    }
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i] = NULL;
    }
    return 0;
}

LogStorage* RsSegmentLogStorage::new_instance(const std::string& uri) const {
    return new RsSegmentLogStorage(uri);
}

void RsSegmentLogStorage::list_files(std::vector<std::string>* seg_files) {
    BAIDU_SCOPED_LOCK(_mutex);
    seg_files->push_back(BRAFT_SEGMENT_META_FILE);
    for (RsSegmentMap::iterator it = _segments.begin(); it != _segments.end(); ++it) {
        scoped_refptr<RsSegment>& segment = it->second;
        seg_files->push_back(segment->file_name());
    }
    if (_open_segment) {
        seg_files->push_back(_open_segment->file_name());
    }
}

void RsSegmentLogStorage::sync() {
    std::vector<scoped_refptr<RsSegment> > segments;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        for (RsSegmentMap::iterator it = _segments.begin(); it != _segments.end(); ++it) {
            segments.push_back(it->second);
        }
    }

    for (size_t i = 0; i < segments.size(); i++) {
        segments[i]->sync(true);
    }
}

scoped_refptr<RsSegment> RsSegmentLogStorage::_open_rs_sgement() {
    scoped_refptr<RsSegment> prev_open_segment;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (!_open_segment) {
            _open_segment = new RsSegment(_path, last_log_index() + 1, _checksum_type);
            if (_open_segment->create() != 0) {
                _open_segment = NULL;
                return NULL;
            }
        }
        if (_open_segment->bytes() > _raft_max_segment_size) {
            _segments[_open_segment->first_index()] = _open_segment;
            prev_open_segment.swap(_open_segment);
        }
    }
    do {
        if (prev_open_segment) {
            if (prev_open_segment->close(_enable_sync) == 0) {
                BAIDU_SCOPED_LOCK(_mutex);
                _open_segment = new RsSegment(_path, last_log_index() + 1, _checksum_type);
                if (_open_segment->create() == 0) {
                    // success
                    break;
                }
            }
            PLOG(ERROR) << "Fail to close old open_segment or create new open_segment"
                        << " path: " << _path;
            // Failed, revert former changes
            BAIDU_SCOPED_LOCK(_mutex);
            _segments.erase(prev_open_segment->first_index());
            _open_segment.swap(prev_open_segment);
            return NULL;
        }
    } while (0);
    return _open_segment;
}

int RsSegmentLogStorage::list_segments(bool is_empty) {
    butil::DirReaderPosix dir_reader(_path.c_str());
    if (!dir_reader.IsValid()) {
        LOG(WARNING) << "directory reader failed, maybe NOEXIST or PERMISSION."
                     << " path: " << _path;
        return -1;
    }

    // restore segment meta
    while (dir_reader.Next()) {
        // unlink unneed segments and unfinished unlinked segments
        if ((is_empty && 0 == strncmp(dir_reader.name(), "log_", strlen("log_"))) ||
            (0 == strncmp(dir_reader.name() + (strlen(dir_reader.name()) - strlen(".tmp")),
                          ".tmp", strlen(".tmp")))) {
            std::string segment_path(_path);
            segment_path.append("/");
            segment_path.append(dir_reader.name());
            ::unlink(segment_path.c_str());

            LOG(WARNING) << "unlink unused segment, path: " << segment_path;

            continue;
        }

        int match = 0;
        int64_t first_index = 0;
        int64_t last_index = 0;
        match = sscanf(dir_reader.name(), BRAFT_SEGMENT_CLOSED_PATTERN, 
                       &first_index, &last_index);
        if (match == 2) {
            LOG(INFO) << "restore closed segment, path: " << _path
                      << " first_index: " << first_index
                      << " last_index: " << last_index;
            RsSegment* segment = new RsSegment(_path, first_index, last_index, _checksum_type);
            _segments[first_index] = segment;
            continue;
        }

        match = sscanf(dir_reader.name(), BRAFT_SEGMENT_OPEN_PATTERN, 
                       &first_index);
        if (match == 1) {
            BRAFT_VLOG << "restore open segment, path: " << _path
                << " first_index: " << first_index;
            if (!_open_segment) {
                _open_segment = new RsSegment(_path, first_index, _checksum_type);
                continue;
            } else {
                LOG(WARNING) << "open segment conflict, path: " << _path
                    << " first_index: " << first_index;
                return -1;
            }
        }
    }

    // check segment
    int64_t last_log_index = -1;
    RsSegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end(); ) {
        RsSegment* segment = it->second.get();
        if (segment->first_index() > segment->last_index()) {
            LOG(WARNING) << "closed segment is bad, path: " << _path
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index != -1 &&
                   segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "closed segment not in order, path: " << _path
                << " first_index: " << segment->first_index()
                << " last_log_index: " << last_log_index;
            return -1;
        } else if (last_log_index == -1 &&
                    _first_log_index.load(butil::memory_order_acquire) 
                    < segment->first_index()) {
            LOG(WARNING) << "closed segment has hole, path: " << _path
                << " first_log_index: " << _first_log_index.load(butil::memory_order_relaxed)
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index == -1 &&
                   _first_log_index > segment->last_index()) {
            LOG(WARNING) << "closed segment need discard, path: " << _path
                << " first_log_index: " << _first_log_index.load(butil::memory_order_relaxed)
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            segment->unlink();
            _segments.erase(it++);
            continue;
        }

        last_log_index = segment->last_index();
        ++it;
    }
    if (_open_segment) {
        if (last_log_index == -1 &&
                _first_log_index.load(butil::memory_order_relaxed) < _open_segment->first_index()) {
        LOG(WARNING) << "open segment has hole, path: " << _path
            << " first_log_index: " << _first_log_index.load(butil::memory_order_relaxed)
            << " first_index: " << _open_segment->first_index();
        } else if (last_log_index != -1 && _open_segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "open segment has hole, path: " << _path
                << " first_log_index: " << _first_log_index.load(butil::memory_order_relaxed)
                << " first_index: " << _open_segment->first_index();
        }
        CHECK_LE(last_log_index, _open_segment->last_index());
    }

    return 0;
}

int RsSegmentLogStorage::load_segments(ConfigurationManager* configuration_manager) {
    int ret = 0;

    // closed segments
    RsSegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end(); ++it) {
        Segment* segment = it->second.get();
        LOG(INFO) << "load closed segment, path: " << _path
            << " first_index: " << segment->first_index()
            << " last_index: " << segment->last_index();
        ret = segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        } 
        _last_log_index.store(segment->last_index(), butil::memory_order_release);
    }

    // open segment
    if (_open_segment) {
        LOG(INFO) << "load open segment, path: " << _path
            << " first_index: " << _open_segment->first_index();
        ret = _open_segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        }
        if (_first_log_index.load() > _open_segment->last_index()) {
            LOG(WARNING) << "open segment need discard, path: " << _path
                << " first_log_index: " << _first_log_index.load()
                << " first_index: " << _open_segment->first_index()
                << " last_index: " << _open_segment->last_index();
            _open_segment->unlink();
            _open_segment = NULL;
        } else {
            _last_log_index.store(_open_segment->last_index(), 
                                 butil::memory_order_release);
        }
    }
    if (_last_log_index == 0) {
        _last_log_index = _first_log_index - 1;
    }
    return 0;
}

int RsSegmentLogStorage::get_segment(int64_t index, scoped_refptr<RsSegment>* ptr) {
    BAIDU_SCOPED_LOCK(_mutex);
    int64_t first_index = first_log_index();
    int64_t last_index = last_log_index();
    if (first_index == last_index + 1) {
        return -1;
    }
    if (index < first_index || index > last_index + 1) {
        LOG_IF(WARNING, index > last_index) << "Attempted to access entry " << index << " outside of log, "
            << " first_log_index: " << first_index
            << " last_log_index: " << last_index;
        return -1;
    } else if (index == last_index + 1) {
        return -1;
    }

    if (_open_segment && index >= _open_segment->first_index()) {
        *ptr = _open_segment;
        CHECK(ptr->get() != NULL);
    } else {
        CHECK(!_segments.empty());
        RsSegmentMap::iterator it = _segments.upper_bound(index);
        RsSegmentMap::iterator saved_it = it;
        --it;
        CHECK(it != saved_it);
        *ptr = it->second;
    }
    return 0;
}

void RsSegmentLogStorage::pop_segments(
        const int64_t first_index_kept,
        std::vector<scoped_refptr<RsSegment> >* popped) {
    popped->clear();
    popped->reserve(32);
    BAIDU_SCOPED_LOCK(_mutex);
    _first_log_index.store(first_index_kept, butil::memory_order_release);
    for (RsSegmentMap::iterator it = _segments.begin(); it != _segments.end();) {
        scoped_refptr<RsSegment>& segment = it->second;
        if (segment->last_index() < first_index_kept) {
            popped->push_back(segment);
            _segments.erase(it++);
        } else {
            return;
        }
    }
    if (_open_segment) {
        if (_open_segment->last_index() < first_index_kept) {
            popped->push_back(_open_segment);
            _open_segment = NULL;
            // _log_storage is empty
            _last_log_index.store(first_index_kept - 1);
        } else {
            CHECK(_open_segment->first_index() <= first_index_kept);
        }
    } else {
        // _log_storage is empty
        _last_log_index.store(first_index_kept - 1);
    }
}

void RsSegmentLogStorage::pop_segments_from_back(
        const int64_t last_index_kept,
        std::vector<scoped_refptr<RsSegment> >* popped,
        scoped_refptr<RsSegment>* last_segment) {
    popped->clear();
    popped->reserve(32);
    *last_segment = NULL;
    BAIDU_SCOPED_LOCK(_mutex);
    _last_log_index.store(last_index_kept, butil::memory_order_release);
    if (_open_segment) {
        if (_open_segment->first_index() <= last_index_kept) {
            *last_segment = _open_segment;
            return;
        }
        popped->push_back(_open_segment);
        _open_segment = NULL;
    }
    for (RsSegmentMap::reverse_iterator 
            it = _segments.rbegin(); it != _segments.rend(); ++it) {
        if (it->second->first_index() <= last_index_kept) {
            // Not return as we need to maintain _segments at the end of this
            // routine
            break;
        }
        popped->push_back(it->second);
        //XXX: C++03 not support erase reverse_iterator
    }
    for (size_t i = 0; i < popped->size(); i++) {
        _segments.erase((*popped)[i]->first_index());
    }
    if (_segments.rbegin() != _segments.rend()) {
        *last_segment = _segments.rbegin()->second;
    } else {
        // all the logs have been cleared, the we move _first_log_index to the
        // next index
        _first_log_index.store(last_index_kept + 1, butil::memory_order_release);
    }
}