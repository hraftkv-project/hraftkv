package site.ycsb.db;

import com.baidu.brpc.protocol.BrpcMeta;
import site.ycsb.db.Cli;

public interface CliService {
    /**
     * To communicate with C++ brpc server, we need to set a BrpcMeta
     * serviceName: <namespace>.<interface>
     * methodName: corresponding function name defined in C++
     * 
     * ref: https://github.com/baidu/brpc-java/blob/master/docs/cn/brpc_server.md
     */
    @BrpcMeta(serviceName = "braft.CliService", methodName = "get_leader")
    Cli.GetLeaderResponse get_leader(Cli.GetLeaderRequest request);
}