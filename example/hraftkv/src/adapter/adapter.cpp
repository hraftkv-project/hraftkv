#include "../config.h"

#include "adapter.h"
#include "rocksdb_adapter.h"

#include "adapter_type.h"

using namespace hraftkv;

adapter::Base* adapter::get_adapter(adapter::Type type) {
    adapter::Base *_apt = NULL;

    switch(type) {
        case adapter::Type::ROCKSDB: {
            const config::RocksDB &rd = config::Config::get_instance().get_rocksdb();
            _apt = adapter::RocksDB::getInstance(rd.dir());
            break;
        }
        default: {
            LOG(ERROR) << "Unknown adapter type: " << config::Config::get_instance().get_apt_type();
            exit(1);
        }
    }

    return _apt;
};