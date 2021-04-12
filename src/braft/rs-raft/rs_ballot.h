#ifndef  RS_RAFT_BALLOT_H
#define  RS_RAFT_BALLOT_H

#include <stdint.h>
#include "braft/configuration.h"

namespace braft {

class RsBallot {
public:
    struct PosHint {
        PosHint() : pos0(-1), pos1(-1) {}
        int pos0;
        int pos1;
    };

    RsBallot();
    ~RsBallot();

    void swap(RsBallot& rhs) {
        _peers.swap(rhs._peers);
        std::swap(_quorum, rhs._quorum);
        _old_peers.swap(rhs._old_peers);
        std::swap(_old_quorum, rhs._old_quorum);
    }

    int init(const Configuration& conf, const Configuration* old_conf, int32_t threshold);
    int init(const Configuration& conf, int32_t threshold);
    PosHint grant(const PeerId& peer, PosHint hint);
    void grant(const PeerId& peer);
    bool granted() const { return _quorum <= 0 && _old_quorum <= 0; }

private:
    struct UnfoundPeerId {
        UnfoundPeerId(const PeerId& peer_id) : peer_id(peer_id), found(false) {}
        PeerId peer_id;
        bool found;
        bool operator==(const PeerId& id) const {
            return peer_id == id;
        }
    };
    std::vector<UnfoundPeerId>::iterator find_peer(
            const PeerId& peer, std::vector<UnfoundPeerId>& peers, int pos_hint) {
        if (pos_hint < 0 || pos_hint >= (int)peers.size()
                || peers[pos_hint].peer_id != peer) {
            for (std::vector<UnfoundPeerId>::iterator
                    iter = peers.begin(); iter != peers.end(); ++iter) {
                if (*iter == peer) {
                    return iter;
                }
            }
            return peers.end();
        }
        return peers.begin() + pos_hint;
    }

    std::vector<UnfoundPeerId> _peers;
    int _quorum;
    std::vector<UnfoundPeerId> _old_peers;
    int _old_quorum;
};

};

#endif  //BRAFT_BALLOT_H
