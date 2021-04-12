#ifndef __BENCHMARK_HH__
#define __BENCHMARK_HH__

#include <vector>
#include <map>
#include <string>
#include <sstream>
#include <stdarg.h>


#include "braft/util.h"
#include "braft/storage.h"
#include "braft/raft.pb.h"

#include "braft/time.h"

#define INVALID_BM_ID -777
#define MAX_LOGMSG_FMT_LENGTH 1024
#define MAX_FILENAME_LENGTH 256
#define INVALID_FSIZE -1

#ifdef PROMETHEUS
#include <prometheus/counter.h>
#include <prometheus/summary.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include "prometheus_reporter.h"

using namespace prometheus;
#endif

namespace bm {

typedef std::pair<std::string, std::string> BmNamePair;
extern bvar::Adder<uint64_t> g_bm_put_data_size;

enum BenchmarkType {
    OP_UNKNOWN = 0,
    OP_KV_GET = 1,
    OP_KV_PUT = 2,
    OP_KV_DEL = 3,
    OP_INSTALL_SNAPSHOT = 9,
    OP_REC_ENTRIES = 10,
    OP_ELECTION = 11,
    OP_LEADER_TRANS = 12,
    OP_SAVE_SNAPSHOT = 13
};

class BaseBMFunc;

/**
 * BaseBMUnit
 */
class BaseBMUnit {
protected:
    unsigned long int _keySize;
    unsigned long int _valueSize;
    BaseBMFunc *_func;                  /* BaseBMFunc that this instance belongs to */

    friend class Benchmark;
    friend class BaseBMFunc;

public:

    /* TagPts  */
    /* overall time */ 
    TagPt overall;

    /* breakdown time */ 
    TagPt clientToServer;
    TagPt performAction;
    TagPt replyToClient;

    BaseBMUnit(); 

    void addKeySize(unsigned long int keySize) {
        _keySize += keySize;
    }

    void addValueSize(unsigned long int valueSize) {
        _valueSize += valueSize;
    }

    unsigned long int getKeySize() {
        return _keySize;
    }

    unsigned long int getValueSize() {
        return _valueSize;
    }

    /**
     * set meta
     * 
     * @param keySize: key size
     * @param valueSize: value size
     * @param func: BaseBMFunc that this instance belongs to
     * 
     * return bool: status
    */
    bool setMeta(unsigned long int keySize, unsigned long int valueSize, BaseBMFunc *func);

    /**
     * check whether the instance is valid
     * 
     * return bool: status
    */
    inline bool isValid() {
        return (_func != NULL);
    }

    /**
     * print log
     * 
     * @param startTime: start TimeVal
     * @param endTime: end TimeVal
     * @param format: additional log message
     * 
     * return string: log message
     * 
    */
    std::string log(const TimeVal &startTime, const TimeVal &endTime, 
                    const char *eventName="", const char *format="", ...);
};

class BaseBMFunc {
protected:
    /* bind to file meta */
    BenchmarkType _type = BenchmarkType::OP_UNKNOWN;
    std::string _funcId;
    std::string _bm_name;
    int _unitId;
    int _num_unit = INVALID_FSIZE;

    std::vector<BaseBMUnit*> *_unitVec;

    friend class Benchmark;

public:
    TagPt overall;

    BaseBMFunc();
    BaseBMFunc(int num_unit);
    virtual ~BaseBMFunc();

    std::map<std::string, double>* calcStats(unsigned long int &, unsigned long int &);

    int addUnit(BaseBMUnit *unit);

    /**
     * allow client to set a custom name to this benchmark function
     * 
     * @param bm_name: custom name
     */
    void set_bm_name(const std::string& bm_name) {
        _bm_name = bm_name;
    }

    /**
     * print log
     * 
     * @param startTime: start TimeVal
     * @param endTime: end TimeVal
     * @param format: additional log message
     * 
     * return string: log message
     * 
    */
    std::string log(const TimeVal &startTime, const TimeVal &endTime, 
                    const char *eventName="", const char *format="", ...);

    /**
     * print out benchmark value map
     * 
     * @param tvMap: value map
     *
     * return stringstream: benchmark result
     */
    std::stringstream printStats(unsigned long int keySizeTotal, 
                    unsigned long int valueSizeTotal, 
                    std::map<std::string, double>* tvMap);
};

class PutUnit : public BaseBMUnit {
public:
    TagPt serializeReq;
    TagPt logCommit;
};

class GetUnit : public BaseBMUnit {
public:
    TagPt getValue;
    TagPt deserialMap;
};

class EntryUnit : public BaseBMUnit { // only use value_size, since entries are specific to key-value pair
public:
    TagPt copyEntries;
    TagPt network;
};

#ifdef PROMETHEUS
class Put : public BaseBMFunc {
public:
    // Prometheus metrics
    Counter& total_key_size;
    Counter& total_value_size;
    Counter& replicate_data_size;
    Counter& raft_log_size;
    Counter& storage_size;
    Summary& storage_io_time;
    Counter& comitted_operations;
    Counter& applied_operations;
    
    // std::map<std::string, double>* calcStats(unsigned long int &, unsigned long int &);
    void start();
    void finish();
    /**
     * Specifically for PUT / GET function
     */
    static Put* create_function(std::string func_id, std::string bm_name);

private:
    Put(
        Counter& total_key_size,
        Counter& total_value_size,
        Counter& replicate_data_size,
        Counter& raft_log_size,
        Counter& storage_size,
        Summary& storage_io_time,
        Counter& comitted_operations,
        Counter& applied_operations
    ) : 
        total_key_size(total_key_size),
        total_value_size(total_value_size),
        replicate_data_size(replicate_data_size),
        raft_log_size(raft_log_size),
        storage_size(storage_size),
        storage_io_time(storage_io_time),
        comitted_operations(comitted_operations),
        applied_operations(applied_operations) {
        _type = BenchmarkType::OP_KV_PUT;
    }
};
#endif

class Get : public BaseBMFunc {
public:
    Get();
    Get(int);
    std::map<std::string, double>* calcStats(unsigned long int &, unsigned long int &);
};

class SaveSnapshot : public BaseBMFunc {
private:
    bool _ready;
    unsigned long int _size;
    int64_t _last_included_term;
    int64_t _last_included_index;

    // copy to the benchmark framework in case it maybe deleted
    braft::SnapshotMeta _meta; 

public:
    TagPt compaction;

    // SaveSnapshot(int64_t last_include_term, int64_t last_included_index);
    SaveSnapshot();
    std::map<std::string, double>* calcStats();

    // void copy_meta(braft::SnapshotMeta& meta) {
    //     _meta = meta;
    // }

    void set_size(size_t size) {
        _size = size;
    }

    size_t get_size() {
        return _size;
    }

    void set_last_included_term(int64_t last_included_term) {
        _last_included_term = last_included_term;
    }

    void set_last_included_index(int64_t last_included_index) {
        _last_included_index = last_included_index;
    }

    int64_t get_last_included_term() {
        return _last_included_term;
    }

    int64_t get_last_included_index() {
        return _last_included_index;
    }

    bool is_ready() {
        return _ready;
    }
};

/**
 * The below benchmark functions will be used when BM_BRAFT is enabled
 *  - InstallSnapshot
 *  - RecoverEntries
 *  - LeaderElection
 *  - LeaderTransfer
 */
class InstallSnapshot : public BaseBMFunc {
private:
    bool _ready;
    std::string _peer_id;
    unsigned long int _size;

public:
    // TagPt overall;
    TagPt loadFromStorage;
    TagPt loadMeta;
    TagPt network;

    InstallSnapshot(std::string _peer_id);
    std::map<std::string, double>* calcStats();

    void set_size(unsigned long int size) {
        _size = size;
    }
    
    unsigned long int getSize() {
        return _size;
    }

    bool is_ready() {
        return _ready;
    }
};

class RecoverEntries : public BaseBMFunc {
private:
    std::string _peer_id;
    std::map<int, EntryUnit*> *_unit_map;
    unsigned long int _size;

public:
    // TagPt overall;

    RecoverEntries(std::string _peer_id);
    ~RecoverEntries();
    std::map<std::string, double>* calcStats();

    void addUnit(int key, EntryUnit* unit);
    size_t numUnit();
    EntryUnit* getUnit(int key);
    std::string printStatus(braft::AppendEntriesRequest *request, const char* format, ...);

    unsigned long int getSize() {
        return _size;
    }
};

class LeaderElection : public BaseBMFunc {
public:
    TimeVal become_leader;
    int64_t leader_priority;
    int64_t end_term;
};

class LeaderTransfer : public BaseBMFunc {
public:
    TimeVal detect_target;
    TagPt transfer;
    bool ready = false;
};

/**
 * Benchmark main class
 */
class Benchmark {

private:

    /**
     * Benchmark function storage map
     * _type2BMMap: used by benchmark function that regardless clients / peer
     * _funcId2BMMap: used by benchmark fuction that unique for different clients / peer
     */
    std::map<BenchmarkType, BaseBMFunc *> _type2BMMap; 
    std::map<std::string, BaseBMFunc *> _funcId2BMMap;

    Benchmark();
    ~Benchmark();

    Benchmark(Benchmark const&); // Don't Implement
    void operator=(Benchmark const&); // Don't implement

#ifdef PROMETHEUS
    PrometheusReporter* _reporter;
#endif

public:
    
    /**
     * Singleton: Instantiated on first use
     */
    static Benchmark& getInstance() {
        static Benchmark instance; // Guaranteed to be destroyed
        return instance; 
    }

    static std::vector<BmNamePair> parse_bm_name(std::string bm_name);

    void clear();

#ifdef PROMETHEUS
    PrometheusReporter& get_reporter() {
        return *_reporter;
    }
#endif

    /**
     * add benchmark function instance to map, 
     * 
     * @param baseBMFunc: benchmark instance
     * @param type: function id, e.g. OP_ELECTION (regardless of clients / peers)
     * <OR>
     * @param funcId: unique identify id (unique for different clients / peers)
     * 
     * @return: reqId
    */
    bool add(BaseBMFunc *baseBMFunc, BenchmarkType type);
    bool add(BaseBMFunc *baseBMFunc, const std::string& funcId);

    /**
     * remove benchmark function instance
     * 
     * @param type: function id, e.g. OP_ELECTION (regardless of clients / peers)
     * <OR>
     * @param funcId: unique identify id (unique for different clients / peers)
     * 
     * @return: success of not
     */
    bool remove(BenchmarkType type);
    bool remove(const std::string& funcId);

    /**
     * get benchmark function instance
     * 
     * @param type: function id, e.g. OP_ELECTION (regardless of clients / peers)
     * <OR>
     * @param funcId: unique identify id (unique for different clients / peers)
     */
    BaseBMFunc *at(BenchmarkType type);
    BaseBMFunc *at(const std::string& funcId);

    /**
     * create an unique id to separate PUT/GET/DEL func
     * format: <uuid>_<func id>
     * example: 087c256a-1265-451a-9b8f-8ae88eda85fa_10
     */
    static std::string bm_func_id(const std::string& uuid, BenchmarkType type);

    // /**
    //  * create an unqiue id for SaveSnapshot func
    //  * format: SaveSnapshot_<last_applied_term>_<last_included_index>
    //  * example: SaveSnapshot_10_140
    //  */
    // std::string save_snapshot_id(const int64_t last_applied_term, const int64_t last_included_index);

    /**
     * create an unique id to separate InstallSnapshot and RecoverEntries func
     * format: <ip>:<port>:<group_id>_<func id>
     * example: 192.168.10.28:8100:0_10
     */
    static std::string recovery_peer_id(const std::string& peer_id, BenchmarkType type);

    /**
     * create an unique id to separate InstallSnapshot and RecoverEntries func
     * format: <ip>:<port>:<group_id>_<func id>
     * example: 192.168.10.28:8100:0_10
     */
    static std::string leader_transfer_peer_id(const std::string& peer_id);

    /**
     * util function to get benchmark unit directory, without the need to
     * get the Benchmark instance and retrieve the corresponding function first
     * 
     * @param type: function id, e.g. OP_ELECTION (regardless of clients / peers)
     * <OR>
     * @param uuid: uuid (unique for different clients / peers)
     * @param type: function id, e.g. OP_ELECTION
     * --> both of the uuid and type construct the unique identify id
     * 
     * @return BaseBMUnit: the benchmark unit to search
     */
    static BaseBMUnit *getUnit(BenchmarkType type, int unitId);
    static BaseBMUnit *getUnit(std::string uuid, BenchmarkType type, int unitId);
};
};

#endif // define __BENCHMARK_HH__
