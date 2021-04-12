package site.ycsb.db;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Arrays;

public class BraftConfiguration {
    private Set<PeerId> _peers;

    public BraftConfiguration() {
        _peers = new HashSet<PeerId>();
    }

    public BraftConfiguration(List<PeerId> peers) {
        _peers = new HashSet<PeerId>();
        _peers.addAll(peers);
    }

    public void reset() {
        _peers.clear();
    }

    public boolean isEmpty() {
        return _peers.isEmpty();
    }

    public int size() {
        return _peers.size();
    }

    public Set<PeerId> listPeers() {
        return _peers;
    }

    public Iterator<PeerId> iterator() {
        return _peers.iterator();
    }

    public boolean add_peer(PeerId peer) {
        return _peers.add(peer);
    }

    public boolean remove_peer(PeerId peer) {
        return _peers.remove(peer);
    }

    public boolean contains(PeerId peer) {
        return _peers.contains(peer);
    }

    public boolean contains(List<PeerId> peers) {
        return _peers.containsAll(peers);
    }

    /**
     * parse cluster configuration string
     * example: 127.0.1.1:8100:0,127.0.1.1:8101:0,127.0.1.1:8102:0,
     */
    public boolean parseFrom(String conf) {
        // clear the original peers
        _peers.clear();

        String[] nodes = conf.split(",");
        for (String node : nodes) {
            PeerId pid = new PeerId();
            if (pid.parse(node)) {
                add_peer(pid);
            } else {
                System.err.println("Cannot parse configuration: " + conf);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BraftConfiguration) {
            BraftConfiguration that = (BraftConfiguration) obj;
            return this._peers.equals(that._peers);
        }
        return false;
    }

    @Override
    public String toString() {
        return _peers.toString();
    }
}

class PeerId {

    private InetSocketAddress addr; // ip+port
    private int idx; // idx in same addr, default 0

    public PeerId() {
        addr = new InetSocketAddress(0);
        idx = 0;
    }

    public PeerId(InetSocketAddress addr_) {
        addr = addr_;
        idx = 0;
    }

    public PeerId(InetSocketAddress addr_, int idx_) {
        addr = addr_;
        idx = idx_;
    }

    public final InetSocketAddress getAddr() {
        return addr;
    }

    public final String getAddrStr() {
        return "list:/" + this.getAddr().toString();
    }

    public final int getId() {
        return idx;
    }

    public void reset() {
        addr = new InetSocketAddress(0);
        idx = 0;
    }

    public boolean isEmpty() {
        return addr.equals(new InetSocketAddress(0));
    }

    public boolean parse(String node_str) {
        reset();

        /**
         * parse node info
         * example: 127.0.1.1:8100:0
         */
        String[] str = node_str.split(":");
        if (str.length != 3) {
            System.err.println("Incorrect format of node string: " + Arrays.toString(str));
            return false;
        } else {
            Integer port = Integer.parseInt(str[1]);
            this.addr = new InetSocketAddress(str[0], port);
            this.idx = Integer.parseInt(str[2]);
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PeerId) {
            PeerId that = (PeerId) obj;
            return this.addr.equals(that.addr) && (this.idx == that.idx);
        }
        return false;
    }

    @Override
    public String toString() {
        return addr.toString() + ":" + idx;
    }
}
