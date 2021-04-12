#ifndef  RS_RAFT_COLLECTION_BOX_H
#define  RS_RAFT_COLLECTION_BOX_H

#include <stdint.h>                             // int64_t
#include <set>                                  // std::set
#include <deque>
#include <map>
#include <butil/atomicops.h>                    // butil::atomic
#include "braft/raft.h"
#include "braft/util.h"

#include "braft/raft.pb.h"                      // FragmentMeta
#include "rs_raft.h"                            // Stripe
#include "rs_ballot.h"                          // RsBallot

namespace braft {

class FSMCaller;
class ClosureQueue;

struct CollectionBoxOptions {
    CollectionBoxOptions() 
        : waiter(NULL)
    {}
    FSMCaller* waiter;
};

struct PendingStripe {
    RsBallot ballot;
    Closure* done;
    butil::IOBuf metadata;
    FragmentMeta fragment_meta;
    std::map<uint32_t, butil::IOBuf>* fragments;

    PendingStripe() {
        fragments = new std::map<uint32_t, butil::IOBuf>();
    }

    // PendingStripe do not delete, as it would pass its fields to Stripe
    // ~PendingStripe() {
        
    // }

    void swap(PendingStripe& rhs) {
        ballot.swap(rhs.ballot);
        done = rhs.done;
        metadata.swap(rhs.metadata);
        fragment_meta.Swap(&rhs.fragment_meta);
        fragments = rhs.fragments;
    }

    void to_stripe(Stripe& stripe) {
        stripe.done = done;
        metadata.swap(stripe.metadata);
        fragment_meta.Swap(&stripe.fragment_meta);
        stripe.fragments = fragments;
        stripe.stripe_data_len = 0U;
    }
};

class CollectionBox {
public:
    CollectionBox();
    ~CollectionBox();

    int init(const CollectionBoxOptions& options);

    int commit_at(int64_t first_collect_index, int64_t last_collect_index, 
        const PeerId& peer, CollectFragmentResponse* response, butil::IOBuf* payload);

    int clear_pending_tasks();
    
    // Called when a candidate becomes the new leader, otherwise the behavior is
    // undefined.
    // According the the raft algorithm, the logs from pervious terms can't be 
    // committed until a log at the new term becomes committed, so 
    // |new_pending_index| should be |last_log_index| + 1.
    int reset_pending_index(int64_t new_pending_index);

    // Called by leader, otherwise the behavior is undefined
    // Store application context before replication.
    int append_pending_task(
        const Configuration& conf, const Configuration* old_conf,
        int64_t collect_index, int32_t threshold, MetadataAndClosure m);

    int64_t last_complete_index() 
    { return _last_complete_index.load(butil::memory_order_acquire); }

private:

    FSMCaller*                                      _waiter;                  
    raft_mutex_t                                    _mutex;
    butil::atomic<int64_t>                          _last_complete_index;
    int64_t                                         _pending_index;
    std::map<int64_t, PendingStripe>*                      _stripe_map;
};

}

#endif  // RS_RAFT_COLLECTION_BOX_H