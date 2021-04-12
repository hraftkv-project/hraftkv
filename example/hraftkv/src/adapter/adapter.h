#ifndef __HRAFTKV_ADAPTER_HH__
#define __HRAFTKV_ADAPTER_HH__

#include <string>
#include <braft/util.h>
#include <pthread.h> // mutex
#include <hraftkv.pb.h>

#include "braft/micro-benchmark/benchmark.h"
#include "adapter_type.h"

#define NIL "(nil)"

namespace hraftkv {
namespace adapter {

// typedef the butil mutex for adapter
typedef ::butil::Mutex adapter_mutex_t;

class Base;

class Base {
public:
    Base() {
        // pthread_mutex_init(&_apt_lk, NULL);
    }

    virtual ~Base() = default;

    /**
     * Get a key value pair from adapter
     *
     * @param[in] key
     * @param[out] out
     *
     * @return bool
     **/
    virtual bool get(const std::string& key, butil::IOBuf& out) = 0;

    /**
     * Put a key value pair into adapter
     *
     * @param[in] key
     * @param[in] in
     *
     * @return whether the key value pair is successfully put
     **/
    virtual bool put(const std::string& key, const butil::IOBuf& in) = 0;

    virtual bool put(const std::string& key, const std::string& in) = 0;

    /**
     * Delete a key value pair from adapter
     *
     * @param[in] key
     *
     * @return whether the key value pair is successfully delete
     **/
    virtual bool del(const std::string& key) = 0;

    virtual size_t get_storage_size() = 0;

};

Base* get_adapter(Type type);

}; // adapter
}; // hraftkv

#endif
