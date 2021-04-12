#include <fstream>                  // file IO
#include <pthread.h>                // mutex
#include <string>
#include <sstream>

#include <brpc/controller.h>        // brpc::Controller
#include <brpc/server.h>            // brpc::Server
#include <brpc/reloadable_flags.h>  // BRPC_VALIDATE_GFLAG
#include <gflags/gflags.h>          // DEFINE_int32

#include "braft/raft.h"             // braft::Node braft::StateMachine
#include "braft/storage.h"          // braft::SnapshotWriter
#include "braft/util.h"             // braft::AsyncClosureGuard
#include "braft/protobuf_file.h"    // braft::ProtoBufFile
#include "braft/priority.h"         // braft::PriorityManagerOptions
#include "braft/micro-benchmark/benchmark.h"

#include "hraftkv.h"
#include "hraftkv.pb.h"             // Various Request RPC
#include "metadata.pb.h"            // GroupTable
#include "multi_raft_controller.h"  // MultiRaftController
#include "state_machine.h"
#include "service.h"                // ServiceImpl
#include "multi_raft_service.h"     // MultiRaftService (Multi-Raft version of ServiceImpl)

#include "config.h"

using namespace hraftkv;

DEFINE_int64(raft_priority, 0, "Node Priority Value");
BRPC_VALIDATE_GFLAG(raft_priority, ::brpc::NonNegativeInteger);

DEFINE_int64(raft_init_leader_priority, 0, "The initial value of Leader Priority");
BRPC_VALIDATE_GFLAG(raft_init_leader_priority, ::brpc::NonNegativeInteger);

int get_priority_by_idx(const std::string& priority_str, int idx) {
    std::istringstream iss(priority_str);
    std::string token;
    std::vector<int> priority_vec;
    while(std::getline(iss, token, ',')) {
        priority_vec.push_back(std::stoi(token));
    }
    int priority = -1;
    try {
        priority = priority_vec.at(idx);
    } catch (std::out_of_range& ex) {
        LOG(ERROR) << "get_priority_by_idx: out of range=" << ex.what();
    }
    return priority;
}

void fill_priority_manager_options_common(
    const config::Config &config,
    braft::PriorityManagerOptions& options) {
    const config::HRaft& h_raft = config.get_h_raft();
    options.enable_ple = h_raft.enable_ple();
    options.enable_pre_vote_ple = h_raft.enable_pre_vote_ple();
    options.enable_leadership_transfer = h_raft.enable_leadership_transfer();
    options.enable_candidate_level = h_raft.enable_candidate_level();
    options.enable_dynamic_priority = h_raft.enable_dynamic_priority();
    options.append_entries_ewma_weight = h_raft.append_entries_ewma_weight();
    options.append_entries_ewma_timeout_ms = h_raft.append_entries_ewma_timeout_ms();
    options.append_entries_latency_thresh_us = h_raft.append_entries_latency_thresh_us();
    options.append_entries_latency_priority_map = h_raft.append_entries_latency_priority_map();
}

bool is_override(std::string flag_name) {
    return !gflags::GetCommandLineFlagInfoOrDie(flag_name.c_str()).is_default;
}

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // invoke config manager to load configuration
    config::Config &config = config::Config::get_instance();
    config.set_config_mode(config::Mode::SERVER);
    config.load_config();

    // generally you only need one Server.
    brpc::Server server;
    
    // get my ip and port
    // butil::EndPoint addr(butil::my_ip(), FLAGS_port);
    // ip_and_port will be used by brpc server class
    std::stringstream ss;
    ss << config.get_server().ip_addr() << ":" << config.get_server().port();
    std::string ip_and_port = ss.str();
    ss << ":" << config.get_server().index();
    std::string ip_port_and_rep_idx = ss.str();
    LOG(INFO) << "[INFO] ip_and_port=" << ip_and_port << " ip_port_and_rep_idx=" << ip_port_and_rep_idx;

    // ip_t will be used by braft node class
    butil::ip_t host_ip;
    if (butil::hostname2ip(std::string(config.get_server().ip_addr()).c_str(), &host_ip) < 0) {
        LOG(ERROR) << "Fail to convert FLAGS_ip_addr to ip_t structure";
        return -1;
    }
    butil::EndPoint addr(host_ip, config.get_server().port());

    std::map<braft::GroupId, NodeResources> id_node_map;
    MetadataManager* _mm = new MetadataManager();
    std::unique_ptr<MetadataManager> mm(_mm);

    /** 
     * Initialization process of of  / 
     * Multi-Raft mode: first Raft group 
     * Single raft mode: the only raft group
     */
    bool is_multi_raft = config.get_h_raft().multi_raft_enable();
    LOG(INFO) << (is_multi_raft ? "[Server] Multi-Raft mode enabled" : "[Server] Single raft mode enabled");
    // Get config of the first group
    const RaftGroup& first_group = config.get_h_raft().multi_raft_groups(0);
    int node_idx = MultiRaftContoller::get_idx_from_conf_str(first_group.conf(), ip_port_and_rep_idx);
    // Create state machine and node instance
    StateMachine* _fsm = new StateMachine();
    NodeArguments node_args;
    node_args.group_id = first_group.id();
    node_args.addr = addr;
    node_args.replication_index = config.get_server().index();
    node_args.conf_str = first_group.conf();
    node_args.fsm = _fsm;
    node_args.election_timeout_ms = get_priority_by_idx(first_group.election_timeout_ms(), node_idx);

    braft::PriorityManagerOptions pmo;
    fill_priority_manager_options_common(config, pmo);
    pmo.initial_priority = is_override("raft_priority") ? FLAGS_raft_priority : get_priority_by_idx(first_group.initial_priority(), node_idx);
    pmo.initial_leader_priority = is_override("raft_init_leader_priority") ? FLAGS_raft_init_leader_priority : first_group.initial_leader_priority();
    // Neglect the proposer in PriorityManagerOptions, as it don't need to be monitored
    LOG(INFO) << "[DEBUG] PeerId=" << ip_port_and_rep_idx << ", group_id=" << first_group.id()
        << ", node_idx=" << node_idx << ", priority override: " << is_override("raft_priority") 
        << ", election_timeout_ms=" << node_args.election_timeout_ms;
    node_args.priority_manager_options = pmo;
    Node* _node = new Node(&node_args);

    // Attach MetadataManager
    _node->attach_metadata_manager(_mm);
    MetadataManagerArg mm_arg;
    mm_arg.node = _node;
    mm_arg.fsm = _fsm;
    mm_arg.is_multi_raft = is_multi_raft;
    _mm->init(&mm_arg);

    // Initialize the MetadataManager
    // Put them into NodeResources for delete
    NodeResources node_res;
    node_res.fsm = _fsm;
    node_res.node = _node;
    id_node_map.insert(std::pair<braft::GroupId, NodeResources>(first_group.id(), node_res));

    // Get the group id
    int res = 0;
    MultiRaftContoller* controller = NULL;
    if (is_multi_raft) { // Multi-Raft
        // Create Group controller
        GroupTable dummy_tbl;
        // Generate a group table for sharding
        // Assume it is generated again each time temporary
        dummy_tbl.set_version(0);
        // Hardcode the shard index and length for YCSB key
        // e.g. user8517097267634966620 or user3032, shard according to the first byte after
        dummy_tbl.set_shard_index(4);
        // Copy the raft group from config to group table
        dummy_tbl.mutable_groups()->Clear();
        dummy_tbl.mutable_groups()->CopyFrom(config.get_h_raft().multi_raft_groups());
        controller = MultiRaftContoller::create_controller_by_table(dummy_tbl);
        if (!controller) {
            LOG(ERROR) << "Unable to initialize GroupTablecontroller, exit";
            return -1;
        }

        const GroupTable& g_tbl = controller->get_group_table();
        // Iterate over all group configuration string
        for (int i = 1; i < g_tbl.groups_size(); i++) {
            const RaftGroup& rg = g_tbl.groups(i);
            // Check whether this group includes "this server"
            const std::string& conf_str = rg.conf();
            // If this group's conf_str doesn't include this server, skip it
            if (conf_str.find(ip_port_and_rep_idx) == std::string::npos) {
                LOG(ERROR) << "conf_str=" << conf_str << " does not contain server's peer_id=" << ip_port_and_rep_idx;
                continue;
            }

            // Get a group id
            int node_idx = MultiRaftContoller::get_idx_from_conf_str(conf_str, ip_port_and_rep_idx);
            // Create state machine and node instance
            StateMachine* fsm = new StateMachine();
            NodeArguments args;
            args.group_id = rg.id();
            args.addr = addr;
            args.replication_index = config.get_server().index();
            args.conf_str = conf_str;
            args.fsm = fsm;
            args.election_timeout_ms = get_priority_by_idx(rg.election_timeout_ms(), node_idx);
            LOG(INFO) << "[DEBUG] group_id=" << args.group_id << ", conf_str=" << args.conf_str;

            // Create PriorityProposer
            braft::PriorityManagerOptions pmo;
            fill_priority_manager_options_common(config, pmo);
            pmo.initial_priority = get_priority_by_idx(rg.initial_priority(), node_idx);
            pmo.initial_leader_priority = rg.initial_leader_priority();
            LOG(INFO) << "[DEBUG] PeerId=" << ip_port_and_rep_idx << ", group_id=" << args.group_id
                << ", node_idx=" << node_idx << ", priority=" << pmo.initial_priority << ", election_timeout_ms=" << args.election_timeout_ms;
            // pmo.proposer = controller->create_priority_proposer(rg.id());
            args.priority_manager_options = pmo;

            Node* node = new Node(&args);
            // Put them into NodeResources
            NodeResources current;
            current.fsm = fsm;
            current.node = node;
            id_node_map.insert(std::pair<braft::GroupId, NodeResources>(rg.id(), current));
        }
        // Create service class
        MultiRaftServiceOptions options;
        options.id_node_map = &id_node_map;
        options.mm = _mm;
        options.controller = controller;
        MultiRaftService* mr_service = new MultiRaftService(options);
        if (server.AddService(mr_service, 
                            brpc::SERVER_OWNS_SERVICE) != 0) {
            LOG(ERROR) << "Fail to add multi raft service";
            res = -1;
            goto quit;
        }
    } else {
        try {
            ServiceImpl* sr_service = new ServiceImpl(_node, _mm);
            if (server.AddService(sr_service, 
                                brpc::SERVER_OWNS_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add single raft service";
                res = -1;
                goto quit;
            }
        } catch (std::out_of_range& err) {
            LOG(ERROR) << "Unable to find NodeResources of group id=" << first_group.id()
                <<  " in single raft mode";
            res = -1;
            goto quit;
        }
    }

    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&server, config.get_server().port()) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        res = -1;
        goto quit;
    }

    // It's recommended to start the server before Counter is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice the default options of server is used here. Check out details from
    // the doc of brpc if you would like change some options;
    if (server.Start(ip_and_port.c_str(), NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        res = -1;
        goto stop_and_quit;
    }

    for (auto it = id_node_map.begin(); it != id_node_map.end(); it++) {
        Node* node = it->second.node;
        CHECK(node != NULL) << "Node is NULL during start";
        if (node->start() != 0) {
            LOG(ERROR) << "Fail to start a node";
            res = -1;
            goto stop_and_quit;
        }
    }

    // Get Benchmark once to intialize it
    bm::Benchmark::getInstance();

    LOG(INFO) << "KVStore service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    // Goto is the best option here as we need to delete all resources 
    // if failure happens but we cannot use unique_ptr
    stop_and_quit: 
    
    LOG(INFO) << "KVStore service is going to quit";
    // Stop application before server
    for (auto it = id_node_map.begin(); it != id_node_map.end(); it++) {
        Node* node = it->second.node;
        node->st();
        // Wait until all the processing tasks are over.
        node->j();
    }
    // Stop the server
    server.Stop(0);
    server.Join();
    // Ask server to remove all services
    server.ClearServices();

    quit:
    // Delete all Node Resources
    for (auto it = id_node_map.begin(); it != id_node_map.end(); it++) {
        NodeResources& current = it->second;
        delete current.fsm;
        delete current.node;
        if (controller) controller->destory_priority_proposer(it->first);
    }

    if (controller) delete controller;

    return res;
}
