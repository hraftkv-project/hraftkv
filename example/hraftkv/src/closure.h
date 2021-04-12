#ifndef __HRAFTKV_CLOSURE_HH__
#define __HRAFTKV_CLOSURE_HH__

#include <brpc/controller.h>       // brpc::Controller

#include "braft/raft.h"            // braft::Closure
#include "hraftkv.pb.h"

namespace hraftkv {
class Node;

class Closure : public braft::Closure {
public:
    Closure(Node* node, 
                const google::protobuf::Message* request, 
                Response* response, 
                google::protobuf::Closure* done) {
        _node = node;
        _request = request;
        _response = response;
        _done = done;
    }
    ~Closure() {}

    inline const google::protobuf::Message* request() const { return _request; }
    inline Response* response() const { return _response; }
    void Run();

protected:
    Node* _node;
    const google::protobuf::Message* _request;
    Response* _response; 
    google::protobuf::Closure* _done;
};

}


#endif
