#include <stdint.h>
#include <string>
#include <braft/util.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "config.h"

#define DEFAULT_STR "NIL"
#define DEFAULT_INT -777

using namespace hraftkv::config;

DEFINE_string(server_conf, SERVER_CONF_NAME, "Path of server config file");
DEFINE_string(client_conf, CLIENT_CONF_NAME, "Path of client config file");

/**
 * Provide gflag interface for temperory override
 */

// Server
DEFINE_string(ip_addr, DEFAULT_STR, "IP address of the interface to be listened");
DEFINE_int32(port, DEFAULT_INT, "Listen port of this peer");
DEFINE_int32(index, 0, "Index of the server in cluster");

// Raft
DEFINE_string(conf, DEFAULT_STR, "Initial configuration of the replication group");
DEFINE_string(data_path, DEFAULT_STR, "Path of data stored on");
DEFINE_int32(election_timeout_ms, DEFAULT_INT,
             "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(vote_timeout_ms, DEFAULT_INT,
             "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(stepdown_timeout_ms, DEFAULT_INT,
             "Start election in such milliseconds if disconnect with the leader");
DEFINE_string(check_term, DEFAULT_STR, "Check if the leader changed to another term"); // use string as boolean
DEFINE_string(disable_cli, DEFAULT_STR, "Don't allow raft_cli access this node");      // use string as boolean
// DEFINE_int32(snapshot_interval, DEFAULT_INT, "Interval between each snapshot");
// TODO: add Multi-Raft arguments here

// Adapter
DEFINE_string(apt_type, DEFAULT_STR, "Adapter type");

// RocksDB
DEFINE_string(rocksdb_dir, DEFAULT_STR, "Path of RocksDB directory");

// RS-Raft setting
DEFINE_string(rs_raft_enable, DEFAULT_STR, "Whether to use erasure coding"); // use string as boolean

Config::Config()
{
    _mode = Mode::UNKNOWN;
    _apt_type = adapter::Type::UNKNOWN;
    _server_path = FLAGS_server_conf;
    _client_path = FLAGS_client_conf;
}

void Config::load_config()
{
    LOG(INFO) << "ConfigManager::_load_config: server config: " << _server_path
              << " client config: " << _client_path;

    if (_mode == Mode::SERVER)
    {
        try
        {
            ini_parser::read_ini(_server_path, _server_pt);
        }
        catch (std::exception &e)
        {
            LOG(ERROR) << "Unable to read server configuration file from=" 
                << _server_path << ", error=" << e.what();
            return;
        }

        // server
        _server.set_ip_addr(is_override("ip_addr") ? FLAGS_ip_addr : _server_pt.get<std::string>("server.ip_addr"));
        _server.set_port(is_override("port") ? FLAGS_port : _server_pt.get<int>("server.port"));
        _server.set_index(is_override("index") ? FLAGS_index : _server_pt.get<int>("server.index"));

        // Raft
        // _raft.set_group(is_override("group") ? FLAGS_group : _server_pt.get<std::string>("raft.group"));
        // _raft.set_conf(is_override("conf") ? FLAGS_conf : _server_pt.get<std::string>("raft.conf"));
        _raft.set_data_path(is_override("data_path") ? FLAGS_data_path : _server_pt.get<std::string>("raft.data_path"));
        // _raft.set_election_timeout_ms(is_override("election_timeout_ms") ? FLAGS_election_timeout_ms : _server_pt.get<int>("raft.election_timeout_ms"));
        _raft.set_stepdown_timeout_ms(is_override("stepdown_timeout_ms") ? FLAGS_stepdown_timeout_ms : _server_pt.get<int>("raft.stepdown_timeout_ms"));
        _raft.set_vote_timeout_ms(is_override("vote_timeout_ms") ? FLAGS_vote_timeout_ms : _server_pt.get<int>("raft.vote_timeout_ms"));
        _raft.set_check_term(is_override("check_term") ? _str2bool(FLAGS_check_term) : _server_pt.get<bool>("raft.check_term"));
        _raft.set_disable_cli(is_override("disable_cli") ? _str2bool(FLAGS_disable_cli) : _server_pt.get<bool>("raft.disable_cli"));
        // _raft.set_snapshot_interval(is_override("snapshot_interval") ? FLAGS_snapshot_interval : _server_pt.get<int>("raft.snapshot_interval"));

        // HRaft
        _h_raft.set_enable_ple(_server_pt.get<bool>("h-raft.enable_ple"));
        // _h_raft.set_initial_priority(_server_pt.get<std::string>("raft.initial_priority"));
        // _h_raft.set_initial_leader_priority(_server_pt.get<int>("raft.initial_leader_priority"));
        _h_raft.set_enable_pre_vote_ple(_server_pt.get<bool>("h-raft.enable_pre_vote_ple"));
        _h_raft.set_enable_leadership_transfer(_server_pt.get<bool>("h-raft.enable_leadership_transfer"));
        _h_raft.set_enable_candidate_level(_server_pt.get<bool>("h-raft.enable_candidate_level"));
        _h_raft.set_enable_dynamic_priority(_server_pt.get<bool>("h-raft.enable_dynamic_priority"));
        _h_raft.set_append_entries_ewma_weight(_server_pt.get<double>("h-raft.append_entries_ewma_weight"));
        _h_raft.set_append_entries_ewma_timeout_ms(_server_pt.get<int>("h-raft.append_entries_ewma_timeout_ms"));
        _h_raft.set_append_entries_latency_thresh_us(_server_pt.get<long>("h-raft.append_entries_latency_thresh_us"));
        _h_raft.set_append_entries_latency_priority_map(_server_pt.get<std::string>("h-raft.append_entries_latency_priority_map"));
        _h_raft.set_multi_raft_enable(_server_pt.get<bool>("h-raft.multi_raft_enable"));
        _h_raft.set_multi_raft_num_groups(_server_pt.get<int>("h-raft.multi_raft_num_groups"));

        // Parse the first_group
        RaftGroup* raft_group = _h_raft.add_multi_raft_groups();
        std::stringstream ss;
        ss << "group_0";
        std::string seg_name = ss.str();
        ss << ".id";
        raft_group->set_id(_server_pt.get<std::string>(ss.str()));
        ss.str("");
        ss.clear();
        ss << seg_name << ".conf";
        raft_group->set_conf(is_override("conf") ? FLAGS_conf : _server_pt.get<std::string>(ss.str()));
        ss.str("");
        ss.clear();
        ss << seg_name << ".initial_priority";
        raft_group->set_initial_priority(_server_pt.get<std::string>(ss.str()));
        ss.str("");
        ss.clear();
        ss << seg_name << ".initial_leader_priority";
        raft_group->set_initial_leader_priority(_server_pt.get<int>(ss.str()));
        ss.str("");
        ss.clear();
        ss << seg_name << ".election_timeout_ms";
        raft_group->set_election_timeout_ms(_server_pt.get<std::string>(ss.str()));

        // Parse the additional groups
        for (int i = 1; _h_raft.multi_raft_enable() && (i <= _h_raft.multi_raft_num_groups()); i++) {
            RaftGroup* raft_group = _h_raft.add_multi_raft_groups();
            std::stringstream ss;
            ss << "group_" << i;
            std::string seg_name = ss.str();
            ss << ".id";
            raft_group->set_id(_server_pt.get<std::string>(ss.str()));
            ss.str("");
            ss.clear();
            ss << seg_name << ".start_key";
            raft_group->set_start_key(_server_pt.get<std::string>(ss.str()));
            ss.str("");
            ss.clear();
            ss << seg_name << ".end_key";
            raft_group->set_end_key(_server_pt.get<std::string>(ss.str()));
            ss.str("");
            ss.clear();
            ss << seg_name << ".conf";
            raft_group->set_conf(_server_pt.get<std::string>(ss.str()));
            ss.str("");
            ss.clear();
            ss << seg_name << ".initial_priority";
            raft_group->set_initial_priority(_server_pt.get<std::string>(ss.str()));
            ss.str("");
            ss.clear();
            ss << seg_name << ".initial_leader_priority";
            raft_group->set_initial_leader_priority(_server_pt.get<int>(ss.str()));
            ss.str("");
            ss.clear();
            ss << seg_name << ".election_timeout_ms";
            raft_group->set_election_timeout_ms(_server_pt.get<std::string>(ss.str()));
        }

        // RsRaft
        // _rs_raft.set_enable(is_override("rs_raft_enable") ? _str2bool(FLAGS_rs_raft_enable) : _server_pt.get<bool>("rs-raft.enable"));

        // adapter
        _adapter.set_type(is_override("apt_type") ? FLAGS_apt_type : _server_pt.get<std::string>("adapter.type"));
        const std::string &apt_str = _adapter.type();
        if (apt_str.compare(ROCKSDB_APT_NAME) == 0)
        {
            _apt_type = adapter::Type::ROCKSDB;
            _rocksdb.set_dir(is_override("rocksdb_dir") ? FLAGS_rocksdb_dir : _server_pt.get<std::string>("rocksdb.dir"));
        }
        else
        {
            LOG(ERROR) << "Unknown adapter type: " << _adapter.type();
            exit(1);
        }
    }
    else if (_mode == Mode::CLIENT)
    {
    }
    else
    {
        LOG(ERROR) << "Unknown configuration mode: " << _mode;
        exit(1);
    }

    LOG(INFO) << *this;
}
