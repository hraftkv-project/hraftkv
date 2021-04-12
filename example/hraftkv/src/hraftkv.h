#ifndef __HRAFTKV_HH__
#define __HRAFTKV_HH__

#define DATA_HEADER_SIZE 4

namespace hraftkv {

enum CommitType {
    COMMIT_UNKNOWN = 0,
    COMMIT_METADATA = 1,
    COMMIT_DATA = 2
};

// Define types for different operation
// Notes: keep this unchange currently, as it aligns with BenchmarkType
enum KVStoreOpType {
    OP_UNKNOWN = 0,
    OP_GET = 1,
    OP_PUT = 2,
    OP_DEL = 3,
    OP_PRINT_BM = 8,
};

struct DataHeader {
    uint32_t entry_type;
};

class Node;
class StateMachine;
class ServiceImpl;
class MetadataManager;
}

#endif
