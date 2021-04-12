package site.ycsb.db;

import java.util.Comparator;
import java.util.List;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.exceptions.RpcException;
import com.baidu.brpc.protocol.Options;
import site.ycsb.db.CliService;
import site.ycsb.db.Cli;

// protobuf
import com.google.protobuf.ByteString;

// brafktv
import hraftkv.Hraftkv.*;
import hraftkv.Metadata.*;

public class BraftRouteTable {

    class GroupResources {
        BraftConfiguration conf;
        PeerId leaderId;
        RpcClient rpcClient;
        HRaftKVService kvService;
        
        GroupResources(BraftConfiguration conf, PeerId leaderId, RpcClient rpcClient, HRaftKVService kvService) {
            this.conf = conf;
            this.leaderId = leaderId;
            this.rpcClient = rpcClient;
            this.kvService = kvService;
        }
    }

    private static final Comparator<byte[]>  keyBytesComparator = BytesUtil.getDefaultByteArrayComparator();
    
    private static final BraftRouteTable _instance = new BraftRouteTable();
    private NavigableMap<byte[], String> _rangeGroupIdMap;         
    private Map<String, GroupResources> _groupResMap;
    private long _groupTableVersion = -1L;
    private int _shardIndex = 0;

    private int _getLeaderRetry = 0;
    private int _getLeaderItv = 1000;

    // brpc
    RpcClientOptions _brpcOpts;

    // private construct to prevent creation
    private BraftRouteTable() {
        // _confMap = new HashMap<String, BraftConfiguration>();
        // _leaderMap = new HashMap<String, PeerId>();
        _rangeGroupIdMap = new TreeMap<>(keyBytesComparator);
        _groupResMap = new HashMap<String, GroupResources>();

        _brpcOpts = new RpcClientOptions();
        _brpcOpts.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        _brpcOpts.setConnectTimeoutMillis(10000);
        _brpcOpts.setWriteTimeoutMillis(1000);
        _brpcOpts.setReadTimeoutMillis(5000);
        _brpcOpts.setMaxTryTimes(10);
        _brpcOpts.setMaxTotalConnections(1000);
        _brpcOpts.setMinIdleConnections(10);
        _brpcOpts.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        _brpcOpts.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
    }

    // get instance method for singleton
    public static BraftRouteTable getInstance() {
        return _instance;
    }

    public void setLeaderRetry(int getLeaderRetry, int getLeaderItv) {
        _getLeaderRetry = getLeaderRetry;
        _getLeaderItv = getLeaderItv;
    }

    public boolean addGroup(String group, String confStr) {
        BraftConfiguration conf = new BraftConfiguration();
        boolean succ = conf.parseFrom(confStr);
        if (!succ) {
            System.out.println("updateConf: conf.parseFrom failed");
            return false;
        }
        return addGroup(group, conf);
    }

    public boolean addGroup(String group, BraftConfiguration conf) {
        if (conf.isEmpty()) {
            System.out.println("addGroup: conf.isEmpty()");
            return false;
        }

        // Get leader peer id from server
        PeerId leaderId = getLeaderOfGroupFromServerWithRetry(group, conf);
        if (leaderId == null) {
            System.out.println("addGroup: leaderId == null");
            return false;
        }

        // Create rpcClient and kvService of this group
        RpcClient rpcClient = new RpcClient(leaderId.getAddrStr(), getRpcConfig());
        HRaftKVService kvService = BrpcProxy.getProxy(rpcClient, HRaftKVService.class);

        // Create the corresponding GroupResources and put to map
        GroupResources res = new GroupResources(conf, leaderId, rpcClient, kvService);
        _groupResMap.put(group, res);
        return true;
    }

    public PeerId getLeaderOfGroupFromServerWithRetry(String group) {
        GroupResources res = _groupResMap.get(group);
        if (res == null) {
            return null;
        }
        return getLeaderOfGroupFromServerWithRetry(group, res.conf);
    }

    public PeerId getLeaderOfGroupFromServerWithRetry(String group, BraftConfiguration conf) {
        PeerId leaderId = null;
        for (int i = 1; i <= _getLeaderRetry; i++) {
            leaderId = _getLeaderOfGroupFromServer(group, conf);
            if (leaderId != null) {
                System.out.println("Get leader successfully, LeaderId: " + leaderId);
                break;
            }
            System.out.println("Failed to get leader in " + i + " trial, wait " + _getLeaderItv + " ms");
            try {
                Thread.sleep(_getLeaderItv);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            if (i == _getLeaderRetry) {
                System.out.println("Failed to get leader, exit");
            }
        }
        return leaderId;
    }
    
    private PeerId _getLeaderOfGroupFromServer(String group, BraftConfiguration conf) {
        // ask every node in the configuration to know who is the leader
        Iterator<PeerId> itr = conf.iterator();
        while (itr.hasNext()) {
            PeerId node = itr.next();
            // create channel to current node
            String addr = "list:/" + node.getAddr().toString();
            System.out.println("Asking peer for leader: " + addr);
            RpcClient rpcClient = new RpcClient(addr, _brpcOpts);
            CliService cliService = BrpcProxy.getProxy(rpcClient, CliService.class);

            // build GetLeaderRequest
            Cli.GetLeaderRequest request = Cli.GetLeaderRequest.newBuilder()
                .setGroupId(group)
                .build();

            // send request and get response
            Cli.GetLeaderResponse response;
            try {
                response = cliService.get_leader(request);
                rpcClient.stop();
                
                // parse response and update leader info
                PeerId leader = new PeerId();
                if (leader.parse(response.getLeaderId())) {
                    System.out.println("Retrieved leader by server: " + leader.toString());
                    return leader;
                } else {
                    System.err.println("Cannot parse leader info: " + response.getLeaderId());
                    return null;
                }
            } catch (RpcException ex) {
                System.err.println("Error occured when send GetLeaderRequest to node: " + ex);
                return null;
            }
        }
        return null;
    }

    public void stopAll() {
        for (GroupResources res : _groupResMap.values()) {
            res.rpcClient.stop();
        }
    }
    
    public boolean disqualifyLeader(String group) {
        GroupResources res = _groupResMap.get(group);
        if (res != null) {
            res.leaderId = null;
        }
        return res != null;
    }

    public HRaftKVService getServiceByGroupId(String groupId) {
        GroupResources res = _groupResMap.get(groupId);
        return res != null ? res.kvService : null;
    }

    public void updateGroupTable(GroupTable groupTable) {
        List<RaftGroup> groupList = groupTable.getGroupsList();
        _shardIndex = groupTable.getShardIndex();
        for (RaftGroup group : groupList) {
            String groupId = group.getId();
            String confStr = group.getConf();
            ByteString startKey = group.getStartKey();
            ByteString endKey = group.getEndKey();
            
            // Now we only use startKey in the map
            _rangeGroupIdMap.put(startKey.toByteArray(), groupId);

            GroupResources oldRes = _groupResMap.get(groupId);
            if (oldRes == null) {
                // Assume all group use the same BraftConfiguration temporary
                addGroup(groupId, confStr);
            }
        }
    }

    public String getGroupIdByKey(String key) {
        String actualKey = key.substring(_shardIndex);
        // System.out.println("ShardIndex=" + _shardIndex + " Actual key=" + actualKey + " Bytes Hex=" + BytesUtil.toHex(actualKey.getBytes()));
        final Map.Entry<byte[], String> entry = _rangeGroupIdMap.floorEntry(actualKey.getBytes());
        return entry != null ? entry.getValue() : "";
    }

    public HRaftKVService getServiceByKey(String key) {
        String groupId = getGroupIdByKey(key);
        if (groupId.equals("")) {
            System.err.println("Unable to find groupId from key=" + key);
            return null;
        }
        return getServiceByGroupId(groupId);
    }
    
    public long getGroupTableVersion() {
        return _groupTableVersion;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<byte[], String> entry : _rangeGroupIdMap.entrySet()) {
            String groupId = entry.getValue();
            builder.append("{GroupId=")
                .append(groupId).append(" Shard key=").append(BytesUtil.toHex(entry.getKey())).append("}\n");
        }
        return builder.toString();
    }

    public boolean isEmpty() {
        return _groupResMap.isEmpty();
    }

    public final RpcClientOptions getRpcConfig() {
        return _brpcOpts;
    }
}