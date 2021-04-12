#include <butil/logging.h>
#include <braft/util.h>
#include <braft/storage.h>               // braft::SnapshotWriter
#include <experimental/filesystem>

// #include "../snapshot/snapshot.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb_adapter.h"

// #include "multi_field.h"

using namespace std::experimental;

using namespace hraftkv::adapter;

RocksDB* RocksDB::getInstance(std::string db_dir) {
    // static RocksDB *_inst = new RocksDB();
    static RocksDB _inst;

    // create the RocksDB instance if not exists
    if (_inst._db == NULL) {
        _inst._db_dir = db_dir;
        _inst._start();
    }

    return &_inst;
}

RocksDB::RocksDB() {
    _db = NULL;
    _options.create_if_missing = true;

    // increase parallel
    _options.IncreaseParallelism();
    // compaction style
    _options.OptimizeLevelStyleCompaction();
}

RocksDB::~RocksDB() {
    _stop();
}

bool RocksDB::get(const std::string& key, butil::IOBuf& out) {
    rocksdb::Status status;
    std::string value;
    status = _db->Get(rocksdb::ReadOptions(), key, &value);

    if (status.ok()) {
        out.append(value);
    } else if (status.IsNotFound()) {
        LOG(ERROR) << "RocksDB::get: key: `" << key << "` is not found";
        return false;
    } else {
        LOG(ERROR) << "RocksDB::get: error occuried when getting the value of key: " << key;
        return false;
    }
    return true;
}

bool RocksDB::put(const std::string& key, const butil::IOBuf& in) {
    rocksdb::Status status;
    std::string value = in.to_string();
    status = _db->Put(rocksdb::WriteOptions(), key, value);
    if (status.ok()) {
        return true;
    }
    LOG(ERROR) << "RocksDB::put: error occuried when putting the value of key: " << key;
    return false;
}

bool RocksDB::put(const std::string& key, const std::string& in) {
    rocksdb::Status status;
    status = _db->Put(rocksdb::WriteOptions(), key, in);
    if (status.ok()) {
        return true;
    }
    LOG(ERROR) << "RocksDB::put: error occuried when putting the value of key: " << key;
    return false;
}

bool RocksDB::del(const std::string& key) {
    rocksdb::Status status = _db->Delete(rocksdb::WriteOptions(), key);
    return status.ok();
}

void RocksDB::_start() {
    // create the directory before create th LevelDB instance
    LOG(INFO) << "RocksDB::_start: recreate leveldb directory " << _db_dir;
    filesystem::create_directory(_db_dir);
    // create the LevelDB instance if not exists
    rocksdb::Status status = rocksdb::DB::Open(_options, _db_dir, &_db);

    if (!status.ok()) {
        LOG(ERROR) << "RocksDB::_start: failed to initialize RocksDB instance";
        exit(1);
    }
}

void RocksDB::_stop() {
    // delete _db;
}

size_t RocksDB::get_storage_size() {
    size_t total_size = 0U;
    try {
        // remove and recreate the leveldb directory
        filesystem::path rocksdb_dir(_db_dir);
        for (filesystem::directory_entry const& entry : filesystem::directory_iterator(rocksdb_dir)) {
            if (filesystem::is_regular_file(entry.status())) {
                total_size += filesystem::file_size(entry.path());
            }
        }
    } catch (filesystem::filesystem_error &ex) {
        LOG(ERROR) << "RocksDB::get_storage_size: " << ex.what();
        return 0U;
    }
    return total_size;
}