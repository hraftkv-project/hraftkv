/**
 * HraftKV client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

// java
import java.sql.Timestamp;
import java.lang.NullPointerException;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.Vector;
import java.util.Properties;
import java.util.Arrays;
import java.lang.Thread;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import static java.nio.charset.StandardCharsets.UTF_8;

import site.ycsb.ByteArrayByteIterator;
// YCSB
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.InputStreamByteIterator;

// protobuf
import com.google.protobuf.ByteString;

// brpc
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.exceptions.RpcException;

// brafktv
import hraftkv.Hraftkv.*;
import hraftkv.Metadata.*;

public class HRaftKVClient extends DB {

    // const
    public static final int REFRESH_TIMEOUT = 1000;

    // braft
    private BraftRouteTable _rt;
    private String _bmName;
    private String _firstGroupId;

    // brpc
    private InetAddress _ip;

    // session id
    private SessionIdResponse _session_id;

    boolean _enableMultiRaft;

    public void init() throws DBException {
        Properties props = getProperties();

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println("Init start timestamp: " + timestamp);
        
        String groupConfStr = null;
        String groupName = null;
        try {
            groupConfStr = props.getProperty("hraftkv.group_conf");
            groupName = props.getProperty("hraftkv.group_id");
        } catch (NullPointerException ex) {
            System.out.println("Missing required fields: " + ex);
        }

        // the time to wait before retry get leader (in ms)
        int getLeaderRetry = Integer.parseInt(props.getProperty("hraftkv.get_leader_retry", "10"));
        int getLeaderItv = Integer.parseInt(props.getProperty("hraftkv.get_leader_interval", "1000"));
        _enableMultiRaft = Boolean.valueOf(props.getProperty("hraftkv.enable_multi_raft", "false"));
        
        // Get local ip adress
        _ip = null;
        try {
            _ip = InetAddress.getLocalHost();
            System.out.println("Include ip address: " + _ip.toString() + " to the request");
        } catch (UnknownHostException ex) {
            System.err.println("Unable to retrieve the ip address, do not include to request: " + ex);
        }
        
        // Check bm_name
        String bmName = props.getProperty("hraftkv.bm_name", "");
        System.out.println("Using benchmark name: " + bmName);
        _bmName = bmName;

        _firstGroupId = groupName;
        System.out.println("Using first groupid=" + _firstGroupId);

        // update route table by configuration string
        BraftRouteTable routeTable = BraftRouteTable.getInstance();
        routeTable.setLeaderRetry(getLeaderRetry, getLeaderItv);
        if (!routeTable.addGroup(_firstGroupId, groupConfStr)) {
            throw new DBException("Unable to add the first group with name=" + _firstGroupId + ", exit");
        }
        _rt = routeTable;

        // get a session id from the cluster
        _session_id = null;
        if (!_getSessionId()) {
            throw new DBException("Unable to get a session id, exit");
        }
        
        // check if the session id response includes group table
        if (_session_id.hasGroupTable()) {
            _rt.updateGroupTable(_session_id.getGroupTable());
            System.out.println("New BraftRouteTable after update: \n" + _rt.toString());
        } else {
            System.out.println("Group table is not foundin SessionIdResponse");
        }

        timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println("Init end timestamp: " + timestamp);
    }

    public void cleanup() throws DBException {
        // expire the uuid
        if (!_expireSessionId()) {
            throw new DBException("Unable to expire the session id, please check");
        }
        // stop rpc client
        // _rpcClient.stop();
        _rt.stopAll();
    }

    private boolean _getSessionId() {
        // construct the session id request
        SessionIdRequest.Builder builder = SessionIdRequest.newBuilder()
            .setOperation(SessionIdRequest.OpType.CREATE)
            .setBmName(_bmName)
            .setIp(_ip == null ? _ip.toString() : "")
            .setGroupTableVersion(_rt.getGroupTableVersion());

        // build request and send
        SessionIdRequest request = builder.build();
        MetadataResponse metaResponse = null;
        try {
            metaResponse = _rt.getServiceByGroupId(_firstGroupId).sessionId(request);
        } catch (RpcException ex) {
            System.err.println("Error occuried when sending session id request: " + ex);
            return false;
        }

        // parse response to result
        if (metaResponse != null && metaResponse.getSuccess()) {
            // extract the SessionIdResponse
            MetadataType type = metaResponse.getType();
            if (type == MetadataType.META_SESSION_ID) {
                _session_id =  metaResponse.getSessionId();
                return true;
            } else {
                System.err.println("Response failed for session id request");
                return false;
            }
        } else {
            System.err.println("Response failed for session id request");
            return false;
        }
    }

    private boolean _expireSessionId() {
        // construct the session id request
        SessionIdRequest.Builder builder = SessionIdRequest.newBuilder()
            .setOperation(SessionIdRequest.OpType.EXPIRE)
            .setUuid(_session_id.getUuid())
            .setIp(_ip == null ? _ip.toString() : "");

        // build request and send
        SessionIdRequest request = builder.build();
        MetadataResponse metaResponse = null;
        try {
            metaResponse = _rt.getServiceByGroupId(_firstGroupId).sessionId(request);
        } catch (RpcException ex) {
            System.err.println("Error occuried when sending session id request: " + ex);
            return false;
        }

        // parse response to result
        if (metaResponse != null && metaResponse.getSuccess()) {
            _session_id = null;
            return true;
        } else {
            System.err.println("Response failed for session id " + _session_id.getUuid() + " expire request");
            return false;
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields,
        Map<String, ByteIterator> result) {

        // construct get request builder
        GetRequest.Builder builder = GetRequest.newBuilder()
            .setSessionId(_session_id.getUuid())
            .setGroupId(_rt.getGroupIdByKey(key))
            .setKey(key);

        // build request and send
        GetRequest request = builder.build();
        Response response = null;
        try {
            response = _enableMultiRaft ? 
                _rt.getServiceByKey(key).get(request) : _rt.getServiceByGroupId(_firstGroupId).get(request);
        } catch (RpcException ex) {
            System.err.println("Error occuried when sending get request: " + ex);
            return Status.ERROR;
        } catch (NullPointerException ex) {
            System.err.println("Error occuried when getServiceByKey: " + ex);
            return Status.ERROR;
        }

        // parse response to result
        if (response != null && response.getSuccess()) {
            result.put("field0", new ByteArrayByteIterator(response.getValue().toByteArray()));
        } else {
            System.err.println("Response failed for get request");
            return Status.ERROR;
        }

        return Status.OK;
    }

    @Override
    public Status insert(String table, String key,
        Map<String, ByteIterator> values) {

        String groupId = _enableMultiRaft ? _rt.getGroupIdByKey(key) : _firstGroupId;
        // System.out.println("Insert key="+key+" to group with id="+groupId);
        // construct put request builder
        PutRequest.Builder builder = PutRequest.newBuilder()
            .setSessionId(_session_id.getUuid())
            .setGroupId(groupId)
            .setKey(key);
        
        builder.setValue(ByteString.copyFrom(values.get("field0").toArray()));

        // build request and send
        PutRequest request = builder.build();
        Response response = null;
        try {
            response = _rt.getServiceByGroupId(groupId).put(request);
        } catch (RpcException ex) {
            System.err.println("Error occuried when sending put request: " + ex);
            return Status.ERROR;
        } catch (NullPointerException ex) {
            System.err.println("Error occuried when getServiceByKey: " + ex);
            return Status.ERROR;
        }

        if (!response.getSuccess()) {
            System.err.println("Operation unsuccessful, disqualify leader of groupId=" + groupId);
            _rt.disqualifyLeader(groupId);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            System.err.println("End sleep");
            _rt.getLeaderOfGroupFromServerWithRetry(groupId);
        }

        return response != null && response.getSuccess() ? Status.OK : Status.ERROR;
    }

    @Override
    public Status delete(String table, String key) {
        // construct delete request
        DeleteRequest request = DeleteRequest.newBuilder()
            .setSessionId(_session_id.getUuid())
            .setGroupId(_rt.getGroupIdByKey(key))
            .setKey(key)
            .build();

        // send request
        Response response = null;
        try {
            response = _enableMultiRaft ? 
                _rt.getServiceByKey(key).del(request) : _rt.getServiceByGroupId(_firstGroupId).del(request);
        } catch (RpcException ex) {
            System.err.println("Error occuried when sending del orequest: " + ex);
            return Status.ERROR;
        } catch (NullPointerException ex) {
            System.err.println("Error occuried when getServiceByKey: " + ex);
            return Status.ERROR;
        }

        return response != null && response.getSuccess() ? Status.OK : Status.ERROR;
    }

    @Override
    public Status update(String table, String key,
        Map<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    @Override
    public Status scan(String table, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        System.out.println("Not yet suport scan");
        return Status.ERROR;
    }
}