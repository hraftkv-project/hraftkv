#ifndef  RS_RAFT_H
#define  RS_RAFT_H

#include <butil/status.h>

namespace braft {
class Closure;

struct Stripe {
    // |done|: delete by itself automatically, do not delete
    Closure* done;
    butil::IOBuf metadata;
    FragmentMeta fragment_meta;
    // |fragments|: can be deleted
    std::map<uint32_t, butil::IOBuf>* fragments;
    // |stripe_data|: used by rs_recover_stripe after recovered the stripe, can be deleted
    unsigned char* stripe_data;
    size_t stripe_data_len;

    // Stripe struct do not create field, because it is converted from PendingStripe
    ~Stripe() {
        if (fragments) delete fragments;
        if (stripe_data) delete stripe_data;
    }

    // TODO: We need to fix it later or change the function name, 
    // as we didn't swap some of the value
    void swap(Stripe& rhs) {
        done = rhs.done;
        metadata.swap(rhs.metadata);
        fragment_meta.Swap(&rhs.fragment_meta);
        stripe_data = rhs.stripe_data;
        stripe_data_len = rhs.stripe_data_len;
        fragments = rhs.fragments;
    }
};

struct Fragment {
    google::protobuf::Closure* done;
    butil::IOBuf metadata;
    butil::IOBuf data;
    FragmentMeta fragment_meta;
};

struct FragmentsAndClosure {
    std::vector<Fragment>* fragments;
    google::protobuf::Closure* done;
    butil::Status status;
};

}

#endif