#include <butil/scoped_lock.h>
#include <bvar/latency_recorder.h>
#include <bthread/unstable.h>
#include "braft/util.h"
#include "braft/fsm_caller.h"
#include "rs_ballot.h"
#include "butil/iobuf.h"

// #include "braft/rs-raft/rs_code.h"

#include "collection_box.h"

namespace braft {
    CollectionBox::CollectionBox()
        : _waiter(NULL)
        , _last_complete_index(0)
        , _pending_index(0)
    {
        _stripe_map = new std::map<int64_t, PendingStripe>();
    }

    CollectionBox::~CollectionBox() {
        clear_pending_tasks();
        delete _stripe_map;
    }

    int CollectionBox::commit_at(int64_t first_collect_index, int64_t last_collect_index, 
        const PeerId& peer, CollectFragmentResponse* response, butil::IOBuf* payload) {
        std::unique_lock<raft_mutex_t> lck(_mutex);
        if (_pending_index == 0) {
            LOG(ERROR) << "CollectionBox::commit_at: EINVAL"; 
            return EINVAL;
        }
        if (last_collect_index < _pending_index) {
            return 0;
        }
        if (last_collect_index >= _pending_index + (int64_t)_stripe_map->size()) {
            LOG(ERROR) << "CollectionBox::commit_at: ENRANGE"; 
            return ERANGE;
        }

        int64_t last_complete_index = 0;
        const int64_t start_at = std::max(_pending_index, first_collect_index);
        RsBallot::PosHint pos_hint;
        for (int64_t idx = start_at; idx <= last_collect_index; ++idx) {
            // Cut payload and insert to the stripe
            const FragmentMeta& fragment_meta = response->meta(idx - start_at);
            try {
                // Retrieve the corresponding stripe
                PendingStripe& stripe = _stripe_map->at(idx);
                // LOG(INFO) << "CollectionBox::commit_at: " << "collect_index=" << idx << " stripe.fragments=" 
                //     << stripe.fragments << " fragment_index()=" << fragment_meta.fragment_index();
                // Cut the fragment data and store to stripe
                butil::IOBuf buf;
                payload->cutn(&buf, fragment_meta.fragment_len());
                stripe.fragments->insert(std::pair<uint32_t, butil::IOBuf>(fragment_meta.fragment_index(), buf));
                // Grant the ballot
                pos_hint = stripe.ballot.grant(peer, pos_hint);
                if (stripe.ballot.granted()) {
                    last_complete_index = idx;
                }
            } catch (const std::out_of_range& ex) {
                LOG(ERROR) << "[RS] CollectionBox::commit_at out of range: " << ex.what() << " collect_index=" << idx 
                    << " fragment_index()=" << fragment_meta.fragment_index();
            }
        }

        if (last_complete_index == 0) {
            return 0;
        }

        std::vector<Stripe> *complete_stripes = new std::vector<Stripe>();
        complete_stripes->reserve(last_complete_index - _pending_index);
        for (int64_t idx = _pending_index; idx <= last_complete_index; ++idx) {
            complete_stripes->push_back(Stripe());
            _stripe_map->at(idx).to_stripe(complete_stripes->back());
            _stripe_map->erase(idx);
        }
        
        // Update _last_complete_index
        _pending_index = last_complete_index + 1;
        _last_complete_index.store(last_complete_index, butil::memory_order_relaxed);
        lck.unlock();

        return 0;
    }

    int CollectionBox::init(const CollectionBoxOptions &options) {
        if (options.waiter == NULL) {
            LOG(ERROR) << "waiter is NULL";
            return EINVAL;
        }
        _waiter = options.waiter;
        return 0;
    }

    int CollectionBox::clear_pending_tasks() {
        {
            BAIDU_SCOPED_LOCK(_mutex);
            _pending_index = 0;
            _stripe_map->clear();
        }
        return 0;
    }

    int CollectionBox::reset_pending_index(int64_t new_pending_index) {
        BAIDU_SCOPED_LOCK(_mutex);
        CHECK(_pending_index == 0 && _stripe_map->empty())
            << "pending_index " << _pending_index << " _stripe_map " 
            << _stripe_map->size();
        CHECK_GT(new_pending_index, _last_complete_index.load(
                                        butil::memory_order_relaxed));
        _pending_index = new_pending_index;
        return 0;
    }

    // Called by leader, otherwise the behavior is undefined
    // Store application context before replication.
    int CollectionBox::append_pending_task(
        const Configuration& conf, const Configuration* old_conf,
        int64_t collect_index, int32_t threshold, MetadataAndClosure m) {
        RsBallot bl;
        if (bl.init(conf, old_conf, threshold) != 0) {
            CHECK(false) << "Fail to init ballot";
            return -1;
        }

        BAIDU_SCOPED_LOCK(_mutex);
        // Create a stripe
        PendingStripe stripe;
        stripe.ballot.swap(bl);
        // Insert the fragment of leader
        // LOG(INFO) << "CollectionBox::append_pending_task: pass 1 collect_index=" << collect_index << " fragment_index=" << m.fragment_meta.fragment_index();
        stripe.fragments->insert(std::pair<uint32_t, butil::IOBuf>(m.fragment_meta.fragment_index(), butil::IOBuf()));
        // Swap the metadata
        stripe.metadata.append(m.metadata);
        // Fill the stripe with data, closure and fragment_meta
        stripe.fragments->at(m.fragment_meta.fragment_index()).append(m.data);
        stripe.done = m.done;
        stripe.fragment_meta.Swap(&m.fragment_meta);
        
        _stripe_map->insert(std::pair<int64_t, PendingStripe>(collect_index, PendingStripe()));
        _stripe_map->at(collect_index).swap(stripe);
        return 0;
    }

} // namespace braft