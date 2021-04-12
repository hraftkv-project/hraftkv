#include "rs_ballot.h"

namespace braft {

RsBallot::RsBallot() : _quorum(0), _old_quorum(0) {}
RsBallot::~RsBallot() {}

int RsBallot::init(const Configuration& conf, const Configuration* old_conf, int32_t threshold) {
    _peers.clear();
    _quorum = 0;
    _old_quorum = 0;

    _peers.reserve(conf.size());
    for (Configuration::const_iterator
            iter = conf.begin(); iter != conf.end(); ++iter) {
        _peers.push_back(*iter);
    }
    _quorum = threshold;

    if (!old_conf) {
        return 0;
    }
    _old_peers.reserve(old_conf->size());
    // LOG(INFO) << "[PLE DEBUG] _old_peers size in Ballot: " << old_conf->size();
    for (Configuration::const_iterator
            iter = old_conf->begin(); iter != old_conf->end(); ++iter) {
        _old_peers.push_back(*iter);
    }
    _old_quorum = threshold;
    
    return 0;
}

RsBallot::PosHint RsBallot::grant(const PeerId& peer, PosHint hint) {
    std::vector<UnfoundPeerId>::iterator iter;
    iter = find_peer(peer, _peers, hint.pos0);
    if (iter != _peers.end()) {
        if (!iter->found) {
            iter->found = true;
            --_quorum;
        }
        hint.pos0 = iter - _peers.begin();
    } else {
        hint.pos0 = -1;
    }

    if (_old_peers.empty()) {
        hint.pos1 = -1;
        return hint;
    }

    iter = find_peer(peer, _old_peers, hint.pos1);

    if (iter != _old_peers.end()) {
        if (!iter->found) {
            iter->found = true;
            --_old_quorum;
        }
        hint.pos1 = iter - _old_peers.begin();
    } else {
        hint.pos1 = -1;
    }

    return hint;
}

void RsBallot::grant(const PeerId& peer) {
    grant(peer, PosHint());
}

}  // namespace braft
