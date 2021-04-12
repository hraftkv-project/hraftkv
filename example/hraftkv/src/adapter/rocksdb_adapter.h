#ifndef __HRAFTKV_ROCKSDB_ADAPTER_HH__
#define __HRAFTKV_ROCKSDB_ADAPTER_HH__

#include "rocksdb/db.h"
#include "rocksdb/options.h"

#include "adapter.h"

namespace hraftkv {
namespace adapter {
class RocksDB : public Base {
public:
    static RocksDB* getInstance(std::string db_dir);

    ~RocksDB();

    bool get(const std::string& key, butil::IOBuf& out);
    bool put(const std::string& key, const butil::IOBuf& in);
    bool put(const std::string& key, const std::string& in);
    bool del(const std::string& key);

    size_t get_storage_size();
    
private:
    RocksDB();

    rocksdb::DB *_db;
    rocksdb::Options _options;

    std::string _db_dir;

    void _stop();
    void _start();
};
};
};

#endif