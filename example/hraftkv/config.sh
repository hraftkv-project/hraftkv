# Source shflags from current directory
mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/../../shflags

###########################################
##### General Variables and Constants #####
###########################################
# Paths 
## although other scripts, such as those in benchmark dir will include
## this config.sh, but won't use these path, since the base is not matched
SCRIPT_PATH=$(readlink -f "$0") # both local and normal
BUILD_DIR=$(dirname "$SCRIPT_PATH") # both local and normal
SRC_DIR=$(dirname "$BUILD_DIR") # both local and normal
BRAFT_DIR=$(dirname $(dirname $(dirname "$SRC_DIR"))) # both local and normal

# Local mode command-line flags
DEFINE_integer port 8100 "Port of the first server"

# Executation settings
DEFINE_string valgrind 0 'Run in valgrind'
DEFINE_string perf 0 'Run in perf'
DEFINE_boolean clean 1 'Remove old "runtime" dir before running'
DEFINE_boolean gdb 0 'Use gdb to debug'
DEFINE_boolean background 0 'Whether run the program in background (must enable for using batch.sh in YCSB)'
DEFINE_boolean local_mode 0 'Whether run_client.sh use local mode' # this flag is only applied to run_client.sh
DEFINE_string runtime_dir './runtime' 'The runtime directory'

##########################
##### Raft settings ######
##########################
DEFINE_boolean raft_sync 1 'Whether force fsync after log append'
DEFINE_string raft_log_storage_type 'rs_segment' 'Type of log storage'

## RocksDB
DEFINE_string rocksdb_dir 'rocksdb' 'Directory of RocksDB'
