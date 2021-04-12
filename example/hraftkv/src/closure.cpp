#include "node.h"
#include "closure.h"

namespace hraftkv {
void Closure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<Closure> self_guard(this);
    // Repsond this RPC.
    brpc::ClosureGuard done_guard(_done);
    _response->set_success(status().ok());
    if (status().ok()) {
        return;
    }
    
    // Try redirect if this request failed.
    _node->redirect(_response);
}

}
