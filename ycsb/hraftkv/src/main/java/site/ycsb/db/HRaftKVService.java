package site.ycsb.db;

import com.baidu.brpc.protocol.BrpcMeta;
import hraftkv.Hraftkv.*;

public interface HRaftKVService {
    /**
     * To communicate with C++ brpc server, we need to set a BrpcMeta
     * serviceName: <namespace>.<interface>
     * methodName: corresponding function name defined in C++
     * 
     * ref: https://github.com/baidu/brpc-java/blob/master/docs/cn/brpc_server.md
     */
    @BrpcMeta(serviceName = "hraftkv.Service", methodName = "get")
    Response get(GetRequest request);

    @BrpcMeta(serviceName = "hraftkv.Service", methodName = "put")
    Response put(PutRequest request);

    @BrpcMeta(serviceName = "hraftkv.Service", methodName = "del")
    Response del(DeleteRequest request);

    @BrpcMeta(serviceName = "hraftkv.Service", methodName = "print_bm")
    Response printBm(PrintBmRequest request);
    
    @BrpcMeta(serviceName = "hraftkv.Service", methodName = "session_id")
    MetadataResponse sessionId(SessionIdRequest request);
}