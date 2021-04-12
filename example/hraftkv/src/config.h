#ifndef __HRAFTKV_CONFIG_HH__
#define __HRAFTKV_CONFIG_HH__

#include <stdint.h>
#include <iostream>
#include <string>
#include <gflags/gflags.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "adapter/adapter_type.h"

#include "config.pb.h"

#define SERVER_CONF_NAME "server.ini"
#define CLIENT_CONF_NAME "client.ini"

using namespace boost::property_tree;

namespace hraftkv {
namespace config {

enum Mode {
    SERVER,
    CLIENT,
    UNKNOWN
};

class Config {
private:
    Config();
    Config(Config const&) = delete;
    void operator=(Config const&) = delete;

    Mode _mode;
    std::string _server_path;
    std::string _client_path;
    
    ptree _server_pt;
    ptree _client_pt;
    
    adapter::Type _apt_type;

    /**
     * Server side configuration
     */
    Server _server;
    Raft _raft;
    HRaft _h_raft;
    RsRaft _rs_raft;
    Adapter _adapter;
    RocksDB _rocksdb;

    bool _str2bool(std::string str) {
        return (str == "true") || (str == "1");
    }

    friend std::ostream & operator<<(std::ostream &os, const Config& c) {
        os << "\n============== Config ==============\n"
        << "[server]\n"
        << " - ip_addr: " << c.get_server().ip_addr() << "\n"
        << " - port: " << c.get_server().port() << "\n"
        << " - index: " << c.get_server().index() << "\n"
        << "[raft]\n"
        << " - data_path: " << c.get_raft().data_path() << "\n"
        << " - stepdown_timeout_ms: " << c.get_raft().stepdown_timeout_ms() << "\n"
        << " - vote_timeout_ms: " << c.get_raft().vote_timeout_ms() << "\n";
        switch(c.get_apt_type()) {
            case adapter::Type::ROCKSDB: {
                os << "[rocksdb]\n"
                   << " - dir: " << c.get_rocksdb().dir() << "\n";
                break;
            }
            default:{}
        }
        for (int i = 0; i < c.get_h_raft().multi_raft_groups_size(); i++) {
            const RaftGroup& group = c.get_h_raft().multi_raft_groups(i);
            os << "[" << group.id() << "]" << "\n"
               << " - start_key: " << group.start_key() << "\n"
               << " - end_key: " << group.end_key() << "\n"
               << " - conf: " << group.conf() << "\n"
               << " - initial_priority: " << group.initial_priority() << "\n"
               << " - initial_leader_priority: " << group.initial_leader_priority() << "\n"
               << " - election_timeout_ms: " << group.election_timeout_ms() << "\n";
        }
        os << "============== Config ==============";
        return os;
    }

public:
    static Config& get_instance() {
        static Config inst;
        return inst;
    }

    const Server& get_server() const {
        return _server;
    }

    const Raft& get_raft() const {
        return _raft;
    }

    const HRaft& get_h_raft() const {
        return _h_raft;
    }

    const RsRaft& get_rs_raft() const {
        return _rs_raft;
    }

    const Adapter& get_adapter() const {
        return _adapter;
    }

    adapter::Type get_apt_type() const {
        return _apt_type;
    }

    const RocksDB & get_rocksdb() const {
        return _rocksdb;
    }

    void set_config_mode(Mode mode) {
        _mode = mode;
    }

    bool is_override(std::string flag_name) {
        return !gflags::GetCommandLineFlagInfoOrDie(flag_name.c_str()).is_default;
    }

    void load_config();
};
};
};

#endif