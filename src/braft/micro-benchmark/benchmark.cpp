#include <cstdio>
#include <limits.h>

#include "braft/util.h"
#include "benchmark.h"

#ifdef PROMETHEUS
#include <prometheus/detail/ckms_quantiles.h>
using namespace prometheus;
#endif

namespace bm {

// std::stringstream bm_put_identifier;
// static void current_put_identifier(std::ostream& os, void*) {
//     os << bm_put_identifier.str();
// }
// static bvar initialization
bvar::IntRecorder g_bm_put_identifier("bm_identifier{type=\"PUT\"}");
bvar::Adder<uint64_t> g_bm_put_data_size("bm_put_data_size");
// bvar::PassiveStatus<std::string> g_bm_put_identifier("bm_put_identifier", current_put_identifier, NULL);

std::vector<std::string> opTypeTable = { 
    "OP_UNKNOWN", 
    "OP_KV_GET", 
    "OP_KV_PUT", 
    "OP_KV_DEL", 
    "", "", "", "", "", 
    "OP_INSTALL_SNAPSHOT",
    "OP_REC_ENTRIES",
    "OP_ELECTION",
    "OP_LEADER_TRANS",
    "OP_SAVE_SNAPSHOT"
};

double byte2MB(const unsigned long int _fileSize) {
    return (_fileSize * 1.0 / (1 << 20));
}

std::string& typeToString(BenchmarkType value) {
    if (value < opTypeTable.size()) {
        return opTypeTable[value];
    } else {
        LOG(ERROR) << "<Benchmark> typeToString BenchmarkType out of range: " << value;
        return opTypeTable[0];
    }
}

std::string log(const TimeVal &startTime, const TimeVal &endTime, 
                                const char *eventName, const char *format, va_list &arglist, const std::string upperLevelStr) {
    std::stringstream ss;
    ss.clear();
    ss << std::setprecision (5);
    ss << "[" << eventName << "] "; // event name

    char _fmtStrBuffer[MAX_LOGMSG_FMT_LENGTH];
    vsnprintf(_fmtStrBuffer, MAX_LOGMSG_FMT_LENGTH, format, arglist);
    if (strlen(_fmtStrBuffer) > 0)
        ss << "\"" << _fmtStrBuffer << "\" ";

    ss << upperLevelStr; // append string

    double usedTime = (endTime - startTime).sec();
    if (usedTime > 0) {
        ss << "time: " << usedTime << "s (" << startTime << ", " << endTime << ") ";
        // if (_size > 0) {
        //     double sizeMB = util::byte2MB(_size);
        //     ss << "size: " << sizeMB << "MB ";
        //     ss << "speed: " << sizeMB / usedTime << "MB/s ";
        // }
    }

    return ss.str();
}

/********************************* BaseBMFunc *****************************************/

BaseBMFunc::BaseBMFunc() {
    _type = BenchmarkType::OP_UNKNOWN;
    _unitVec = new std::vector<bm::BaseBMUnit*>();
    _unitId = 0;
}

BaseBMFunc::BaseBMFunc(int num_unit) {
    _type = BenchmarkType::OP_UNKNOWN;
    _unitVec = new std::vector<BaseBMUnit*>(num_unit);
    _num_unit = num_unit;
    _unitId = 0;
}

BaseBMFunc::~BaseBMFunc() {
    delete _unitVec;
}

int BaseBMFunc::addUnit(BaseBMUnit *unit) {
    if (_num_unit > 0) {
        try {
            _unitVec->at(_unitId) = unit;
        } catch (std::out_of_range ex) {
            LOG(ERROR) << "_funcId: " << _funcId << " _unitVec out of range, current unitId: " << _unitId;
        }
    } else {
        _unitVec->push_back(unit);
    }

    int res = _unitId;
    _unitId++;
    
    return res;
}

std::string BaseBMFunc::log(const TimeVal &startTime, const TimeVal &endTime, 
                                const char *eventName, 
                                const char *format, ...) {

    std::stringstream ss;
    ss.clear();

    va_list arglist;
    va_start(arglist, format);

    std::string finalStr = log(startTime, endTime, eventName, format, arglist, ss.str());
    va_end(arglist);

    return finalStr;
}

std::map<std::string, double>* BaseBMFunc::calcStats(unsigned long int &keySizeTotal, unsigned long int &valueSizeTotal) {
    std::map<std::string, double>* tvMap = new std::map<std::string, double>();

    keySizeTotal = 0L;
    valueSizeTotal = 0L;
    double unitOverall = 0.0;
    double performAction = 0.0;
    TagPt overallTagPt;

    int unitId = 0;
    for (auto it = _unitVec->begin(); it != _unitVec->end(); it++, unitId++) {
        BaseBMUnit *unit = *it;
        // LOG(INFO) << "<Benchmark> processing unitId: " << unitId << " start: " 
        //     << unit->overall.getStart() << " end: " << unit->overall.getEnd();

        keySizeTotal += unit->_keySize;
        valueSizeTotal += unit->_valueSize;
        unitOverall += unit->overall.usedTime();
        performAction += unit->performAction.usedTime();

        if (it == _unitVec->begin()) {
            LOG(INFO) << "<Benchmark>: begin unit found: " << unitId;
            overallTagPt.setStart(unit->overall.getStart());
        }

        if (it == (_unitVec->end() - 1)) {
            LOG(INFO) << "<Benchmark>: end unit found: " << unitId;
            overallTagPt.setEnd(unit->overall.getEnd());
        }
    }

    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("Overall"), overallTagPt.usedTime()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("OverallSecond", "s"), overallTagPt.usedTimeS()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("UnitOverall"), unitOverall / _unitVec->size()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("PerformAction"), performAction / _unitVec->size()));

    return tvMap;
}

// Align with YCSB results
std::stringstream BaseBMFunc::printStats(unsigned long int keySizeTotal, 
                            unsigned long int valueSizeTotal, 
                            std::map<std::string, double>* tvMap) {

    unsigned long int size = keySizeTotal + valueSizeTotal;
    double sizeMB = byte2MB(size);

    // retrieve overall from map
    auto itr = tvMap->find(TagPt::embedUnit("Overall"));
    double overall = itr->second;
    tvMap->erase(TagPt::embedUnit("Overall"));
    // retrieve overall second from map
    itr = tvMap->find(TagPt::embedUnit("OverallSecond", "s"));
    double overallSecond = itr->second;
    tvMap->erase(TagPt::embedUnit("OverallSecond", "s"));

    std::stringstream ss;
    ss.clear();

    ss << std::fixed << std::setprecision(3) << "\n"
        << "[MICRO], BenchmarkType, " << typeToString(_type) << "\n"
        << "[MICRO], FunctionId, " << _funcId << "\n"
        << "[MICRO], TotalSize(MiB), " << sizeMB << "\n"
        << "[MICRO], TotalTime(ms), " << overall << "\n"
        << "[MICRO], Throughput(MiB/s), " << sizeMB / overallSecond << "\n";

    if (tvMap != NULL) { // it maybe null, since no information needs to be print
        size_t maxLength = 0;
        for (auto const& x : *tvMap) {
            maxLength = x.first.length() > maxLength ? x.first.length() : maxLength;
        }

        for (auto const& x : *tvMap) {
            ss << "[MICRO_SUBTASK], " << x.first << ", " << x.second << "\n";
        }
    }

    LOG(INFO) << ss.str();
    return ss;
}


/********************************* BaseBMUnit *****************************************/

BaseBMUnit::BaseBMUnit() {
    _func = NULL;
}

bool BaseBMUnit::setMeta(unsigned long int keySize, unsigned long int valueSize, BaseBMFunc *func) {
    _keySize = keySize;
    _valueSize = valueSize;
    _func = func;
    
    return true;
}

std::string BaseBMUnit::log(const TimeVal &startTime, const TimeVal &endTime, 
                                const char *eventName, 
                                const char *format, ...) {

    std::stringstream ss;
    ss.clear();

    va_list arglist;
    va_start(arglist, format);

    ss << "keySize: " << _keySize << " ";
    ss << "valueSize: " << _valueSize << " ";

    std::string finalStr = log(startTime, endTime, eventName, format, arglist, ss.str());
    va_end(arglist);
    
    return finalStr;
}

/********************************* Cusomized Func *****************************************/

#ifdef PROMETHEUS
void Put::start() {
    // clear the adder
    g_bm_put_data_size.reset();
    g_bm_put_identifier.reset();
    g_bm_put_identifier << 1;
}

void Put::finish() {
    replicate_data_size.Increment(g_bm_put_data_size.get_value());
    // clear the adder
    g_bm_put_data_size.reset();
}

Put* Put::create_function(std::string session_id, std::string bm_name) {
    Benchmark& bm = Benchmark::getInstance();
    // parse bm_name
    std::vector<BmNamePair> pairs = bm.parse_bm_name(bm_name);
    // create counter for put
    std::map<std::string, std::string> common_fields({{"func_type", "put"}, {"session_id", session_id}});
    for (auto it = pairs.begin(); it != pairs.end(); it++) {
        BmNamePair& pair = *it;
        common_fields.insert(pair);
    }
    common_fields.insert({"metric_name", "total_key_size"});
    Counter& total_key_size = bm.get_reporter().counter_family.Add(common_fields);
    common_fields.erase("metric_name");
    common_fields.insert({"metric_name", "total_value_size"});
    Counter& total_value_size = bm.get_reporter().counter_family.Add(common_fields);
    common_fields.erase("metric_name");
    common_fields.insert({"metric_name", "replicate_data_size"});
    Counter& replicate_data_size = bm.get_reporter().counter_family.Add(common_fields);
    common_fields.erase("metric_name");
    common_fields.insert({"metric_name", "raft_log_size"});
    Counter& raft_log_size = bm.get_reporter().counter_family.Add(common_fields);
    common_fields.erase("metric_name");
    common_fields.insert({"metric_name", "storage_size"});
    Counter& storage_size = bm.get_reporter().counter_family.Add(common_fields);
    common_fields.erase("metric_name");
    common_fields.insert({"metric_name", "storage_io_time"});
    // std::unique_ptr<Summary> storage_io_name_ptr();
    Summary& storage_io_name = bm.get_reporter().summary_family.Add(common_fields, Summary::Quantiles{{0.5, 0.1}});
    common_fields.erase("metric_name");
    common_fields.insert({"metric_name", "comitted_operations"});
    Counter& comitted_operations = bm.get_reporter().counter_family.Add(common_fields);
    common_fields.erase("metric_name");
    common_fields.insert({"metric_name", "applied_operations"});
    Counter& applied_operations = bm.get_reporter().counter_family.Add(common_fields);
    Put* func = new Put(
                    total_key_size,
                    total_value_size,
                    replicate_data_size,
                    raft_log_size,
                    storage_size,
                    storage_io_name,
                    comitted_operations,
                    applied_operations
                );
    return func;
}
#endif

Get::Get() : BaseBMFunc() {
    _type = BenchmarkType::OP_KV_GET;
}

Get::Get(int _num_unit) : BaseBMFunc(_num_unit) {
    _type = BenchmarkType::OP_KV_GET;
}

std::map<std::string, double>* Get::calcStats(unsigned long int &keySizeTotal, unsigned long int &valueSizeTotal) {
    std::map<std::string, double> *tvMap = BaseBMFunc::calcStats(keySizeTotal, valueSizeTotal);

    double deserialMap = 0.0;
    double getValue = 0.0;

    int i = 0;
    for (auto it = _unitVec->begin(); it != _unitVec->end(); it++) {
        GetUnit *unit = static_cast<GetUnit*>(*it);
        LOG(INFO) << "unit " << (i++) << ": deserialMap: " << unit->deserialMap.usedTime() << " getValue: " << unit->getValue.usedTime();
        deserialMap += unit->deserialMap.usedTime();
        getValue += unit->getValue.usedTime();
    }

    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("DeserialMap"), deserialMap / _unitVec->size()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("GetValue"), getValue / _unitVec->size()));

    return tvMap;
}

/********************************* SaveSnapshot *****************************************/
SaveSnapshot::SaveSnapshot() : BaseBMFunc() {
    _ready = false;
    _size = 0UL;
    _last_included_term = 0L;
    _last_included_index = 0L;
    _type = BenchmarkType::OP_SAVE_SNAPSHOT;
}

std::map<std::string, double>* SaveSnapshot::calcStats() {
    std::map<std::string, double>* tvMap = new std::map<std::string, double>();
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("Compaction"), compaction.usedTime()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("Overall"), overall.usedTime()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("OverallSecond", "s"), overall.usedTimeS()));

    _ready = true;

    return tvMap;
}

/********************************* InstallSnapshot *****************************************/
InstallSnapshot::InstallSnapshot(std::string peer_id) : BaseBMFunc() {
    _ready = false;
    _size = 0UL;
    _peer_id = peer_id;
    _type = BenchmarkType::OP_INSTALL_SNAPSHOT;
}

std::map<std::string, double>* InstallSnapshot::calcStats() {
    std::map<std::string, double>* tvMap = new std::map<std::string, double>();
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("LoadFromStorage"), loadFromStorage.usedTime()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("LoadMeta"), loadMeta.usedTime()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("Network"), network.usedTime()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("Overall"), overall.usedTime()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("OverallSecond", "s"), overall.usedTimeS()));

    _ready = true;

    return tvMap;
}

/********************************* RecoverEntries *****************************************/
RecoverEntries::RecoverEntries(std::string peer_id) : BaseBMFunc() {
    _size = 0UL;
    _peer_id = peer_id;
    _type = BenchmarkType::OP_REC_ENTRIES;
    _unit_map = new std::map<int, EntryUnit*>();
}

RecoverEntries::~RecoverEntries() {
    delete _unit_map;
}

std::map<std::string, double>* RecoverEntries::calcStats() {
    std::map<std::string, double>* tvMap = new std::map<std::string, double>();

    _size = 0L;
    TimeVal startTime(0, 0);
    TimeVal endTime(0, 0);
    TagPt startToEnd;
    double copyEntries = 0.0;
    double network = 0.0;

    if (_unit_map->size() == 0) {
        LOG(ERROR) << "RecoverEntries::calcStats: no unit exists, error";
    } else {
        for (auto it = _unit_map->begin(); it != _unit_map->end(); it++) {
            EntryUnit *unit = it->second;
            int unitId = it->first;

            // skip unit that sent zero byte
            if (unit->getValueSize() == 0) {
                LOG(INFO) << "RecoverEntries::calcStats: skip zero byte unit, id: " << unitId;
                continue; 
            }

            if (_unit_map->size() == 1) {
                LOG(INFO) << "RecoverEntries::calcStats: only has one unit, id: " << unitId;

                startTime = unit->overall.getStart();
                endTime = unit->overall.getEnd();
            } else {
                if ((unitId % 1000) == 0)
                    LOG(INFO) << "RecoverEntries::calcStats: processing unitId: " << unitId << ", size: " << unit->getValueSize();
                if (it == _unit_map->begin()) {
                    startTime = unit->overall.getStart();
                    LOG(INFO) << "RecoverEntries::calcStats: startTime: " << startTime << " unitId: " << unitId; 
                } else if (it == (--_unit_map->end())) {
                    endTime = unit->overall.getEnd();
                    LOG(INFO) << "RecoverEntries::calcStats: endTime: " << endTime << " unitId: " << unitId; 
                }
            }

            _size += unit->getValueSize(); // only use the valueSize as size;
            copyEntries += unit->copyEntries.usedTime();
            network += unit->network.usedTime();
        }
    }

    startToEnd.setStart(startTime);
    startToEnd.setEnd(endTime);

    // set to overall time
    overall.setStart(startToEnd.getStart());
    overall.setEnd(startToEnd.getEnd());

    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("Overall"), startToEnd.usedTime()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("OverallSecond", "s"), startToEnd.usedTimeS()));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("CopyEntries"), copyEntries));
    tvMap->insert(std::pair<std::string, double>(TagPt::embedUnit("Network"), network));

    return tvMap;
}

void RecoverEntries::addUnit(int key, EntryUnit* unit) {
    _unit_map->insert(std::pair<int, EntryUnit*>(key, unit));
}

size_t RecoverEntries::numUnit() {
    return _unit_map->size();
}

EntryUnit* RecoverEntries::getUnit(int key) {
    auto itr = _unit_map->find(key);
    if (itr == _unit_map->end()) {
        LOG(ERROR) << "<Benchmark> RecoverEntries::getUnit: unit not found, key " << key;
        return NULL;
    }

    return (*itr).second;
}

std::string RecoverEntries::printStatus(braft::AppendEntriesRequest *request, const char* format, ...) {
    va_list args;
    va_start (args, format);
    char msg[4096];
    vsprintf(msg, format, args);

    std::stringstream ss;
    ss << "\n===== <Benchmark> " << typeToString(_type) << " =====\n"
       << " - peer_id: " << _peer_id << "\n"
       << " - committed_index: " << request->committed_index() << "\n"
       << " - prev_index: " << request->prev_log_index() << "\n"
       << " - count: " << request->entries_size() << "\n"
       << " - msg: " << msg << "\n"
       << "=======================";

    va_end(args);
    return ss.str();
}

/********************************* Benchmark *****************************************/
Benchmark::Benchmark() {
#ifdef PROMETHEUS
    auto registry = std::make_shared<Registry>();
    // Prometheus initialization
    auto& counter_family = BuildCounter()
                            .Name("bm_counter")
                            .Help("Benchmark counter family")
                            .Register(*registry);
    auto& gauge_family = BuildGauge()
                            .Name("bm_gauge")
                            .Help("Benchmark gauge family")
                            .Register(*registry);
    auto& summary_family = BuildSummary()
                            .Name("bm_summary")
                            .Help("Benchmark summary family")
                            .Register(*registry);
    _reporter = new PrometheusReporter(registry, counter_family, gauge_family, summary_family);
#endif
}

Benchmark::~Benchmark() {
#ifdef PROMETHEUS
    delete _reporter;
#endif
}

void Benchmark::clear() {
    _type2BMMap.clear();
}

std::vector<BmNamePair> Benchmark::parse_bm_name(std::string bm_name) {
    std::vector<BmNamePair> output;
    std::istringstream ss(bm_name);
    std::string token;
    while(std::getline(ss, token, ',')) {
        int pos = token.find(':', 0);
        std::string field(token.substr(0, pos));
        std::string value(token.substr(pos + 1, token.length() - pos));
        BmNamePair pair(field, value);
        output.push_back(pair);
    }
    return output;
}

bool Benchmark::add(BaseBMFunc *baseBMFunc, BenchmarkType type) {
    std::map<BenchmarkType, BaseBMFunc *>::iterator it = _type2BMMap.find(type);
    if (it != _type2BMMap.end()) {
        LOG(ERROR) << "found existing type";
        return false;
    }

    // set function id for BMFunc
    baseBMFunc->_funcId = typeToString(type);
    // insert to map
    _type2BMMap.insert(std::pair<BenchmarkType, BaseBMFunc *>(type, baseBMFunc));
    
    return true;
}

bool Benchmark::add(BaseBMFunc *baseBMFunc, const std::string& funcId) {
    std::map<std::string, BaseBMFunc *>::iterator it = _funcId2BMMap.find(funcId);
    if (it != _funcId2BMMap.end()) {
        LOG(ERROR) << "found existing funcId";
        return false;
    }

    // set function id for BMFunc
    baseBMFunc->_funcId = funcId;
    // insert to map
    _funcId2BMMap.insert(std::pair<std::string, BaseBMFunc *>(funcId, baseBMFunc));
    
    return true;
}

bool Benchmark::remove(BenchmarkType type) {
    std::map<BenchmarkType, BaseBMFunc *>::iterator it = _type2BMMap.find(type);
    if (it == _type2BMMap.end()) {
        LOG(ERROR) << "cannot find type";
        return false;
    }
    _type2BMMap.erase(it);

    return true;
}

bool Benchmark::remove(const std::string& funcId) {
    std::map<std::string, BaseBMFunc *>::iterator it = _funcId2BMMap.find(funcId);
    if (it == _funcId2BMMap.end()) {
        LOG(ERROR) << "cannot find funcId";
        return false;
    }
    _funcId2BMMap.erase(it);

    return true;
}

BaseBMFunc *Benchmark::at(BenchmarkType type) {
    std::map<BenchmarkType, BaseBMFunc *>::iterator it = _type2BMMap.find(type);
    if (it == _type2BMMap.end()) {
        return NULL;
    }
    return it->second;
}

BaseBMFunc *Benchmark::at(const std::string& funcId) {
    std::map<std::string, BaseBMFunc *>::iterator it = _funcId2BMMap.find(funcId);
    if (it == _funcId2BMMap.end()) {
        return NULL;
    }
    return it->second;
}


/**
 * create an unique id to separate PUT/GET/DEL func
 * format: <uuid>_<func id>
 * example: 087c256a-1265-451a-9b8f-8ae88eda85fa_10
 */
std::string Benchmark::bm_func_id(const std::string& uuid, BenchmarkType type) {
    std::stringstream ss;
    ss << uuid << "_" << type;
    // LOG(INFO) << "bm_func_id: " << uuid << "_" << type;
    return ss.str();
}

// /**
//  * create an unqiue id for SaveSnapshot func
//  * format: SaveSnapshot_<last_applied_term>_<last_included_index>
//  * example: SaveSnapshot_10_140
//  */
// std::string Benchmark::save_snapshot_id(const int64_t last_applied_term, const int64_t last_included_index) {
//     std::stringstream ss;
//     ss << "SaveSnapshot_" << last_applied_term << "_" << last_included_index;
//     return ss.str();
// }

/**
 * create an unique id to separate InstallSnapshot and RecoverEntries func
 * format: <ip>:<port>:<group_id>_<func id>
 * example: 192.168.10.28:8100:0_10
 */
std::string Benchmark::recovery_peer_id(const std::string& peer_id, BenchmarkType type) {
    std::stringstream ss;
    ss << peer_id << "_" << type;
    // LOG(INFO) << "recovert_peer_id: " << ss.str();
    return ss.str();
}

/**
 * create an unique id to separate InstallSnapshot and RecoverEntries func
 * format: <ip>:<port>:<group_id>_<func id>
 * example: 192.168.10.28:8100:0_10
 */
std::string Benchmark::leader_transfer_peer_id(const std::string& peer_id) {
    std::stringstream ss;
    ss << peer_id << "_" << BenchmarkType::OP_LEADER_TRANS;
    // LOG(INFO) << "recovert_peer_id: " << ss.str();
    return ss.str();
}

BaseBMUnit *Benchmark::getUnit(BenchmarkType type, int unitId) {
    bm::Benchmark &bm = bm::Benchmark::getInstance();
    BaseBMUnit* unit = NULL;
    try {
        bm::BaseBMFunc *func = bm.at(type);
        unit = func == NULL ? NULL : func->_unitVec->at(unitId);
    } catch (const std::out_of_range& oor) {
        LOG(ERROR) << "Benchmark::getUnit out of Range: " << oor.what();
        return NULL;
    }
    return unit;
}

BaseBMUnit *Benchmark::getUnit(std::string uuid, BenchmarkType type, int unitId) {
    bm::Benchmark &bm = bm::Benchmark::getInstance();
    BaseBMUnit* unit = NULL;
    try {
        bm::BaseBMFunc *func = bm.at(bm.bm_func_id(uuid, type));
        unit = func == NULL ? NULL : func->_unitVec->at(unitId);
    } catch (const std::out_of_range& oor) {
        LOG(ERROR) << "Benchmark::getUnit out of Range: " << oor.what();
        return NULL;
    }
    return unit;
}

} // namespace bm
