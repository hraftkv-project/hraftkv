cmake_minimum_required(VERSION 3.12)
project(kvstore C CXX)

option(LEVELDB_WITH_SNAPPY "Whether the leveldb is compiled with snappy" ON)
option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" OFF)
option(LINK_TCMALLOC "Link tcmalloc if possible" ON)
option(BRPC_WITH_GLOG "Find and link glog (necessary when used with nCloud)" OFF)
option(MICRO_BM "Enable micro-benchmark framework" OFF)

execute_process(
    COMMAND bash -c "find ${CMAKE_SOURCE_DIR}/../.. -type d -path \"*output/include/braft\" | xargs dirname | xargs dirname | tr -d '\n'"
    OUTPUT_VARIABLE OUTPUT_PATH
)

set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

include(FindThreads)
include(FindProtobuf)
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

#############
### Paths ###
#############

# Global dependencies, link flags
set ( ALLDEPS  )
# Add cmake module path
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
# Setup third-party library (We don't need it now)
# set ( PROJECT_THIRD_PARTY_LIB_DIR ${PROJECT_SOURCE_DIR}/third-party )
# set ( THIRD_PARTY_INCLUDE_CONFIG ${PROJECT_BINARY_DIR}/include )
# set ( THIRD_PARTY_LIB_CONFIG ${PROJECT_BINARY_DIR}/lib )
# link_directories(${THIRD_PARTY_LIB_CONFIG})

############################
### Library dependencies ###
############################

## protobuf
if (NOT PROTOBUF_PROTOC_EXECUTABLE)
    get_filename_component(PROTO_LIB_DIR ${PROTOBUF_LIBRARY} DIRECTORY)
    set (PROTOBUF_PROTOC_EXECUTABLE "${PROTO_LIB_DIR}/../bin/protoc")
endif()

# generate protobuf header
file(GLOB PROTO_RAW "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/*.proto")
protobuf_generate_cpp(PROTO_SRC PROTO_HEADER ${PROTO_RAW})

# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})

## brpc && braft
find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
if(EXAMPLE_LINK_SO)
    find_library(BRPC_LIB NAMES brpc)
    find_library(BRAFT_LIB NAMES braft)
else()
    find_library(BRPC_LIB NAMES libbrpc.a brpc)
    find_library(BRAFT_LIB NAMES libbraft.a braft)
endif()

if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()
include_directories(${BRPC_INCLUDE_PATH})

find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h)
if ((NOT BRAFT_INCLUDE_PATH) OR (NOT BRAFT_LIB))
    message (FATAL_ERROR "Fail to find braft")
endif()
include_directories(${BRAFT_INCLUDE_PATH})

## gflags
find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

execute_process(
    COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
    OUTPUT_VARIABLE GFLAGS_NS
)
if(${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    execute_process(
        COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
    )
endif()

## tcmalloc
if (LINK_TCMALLOC)
    find_path(GPERFTOOLS_INCLUDE_DIR NAMES gperftools/heap-profiler.h)
    find_library(GPERFTOOLS_LIBRARIES NAMES tcmalloc_and_profiler)
    if (GPERFTOOLS_INCLUDE_DIR AND GPERFTOOLS_LIBRARIES)
        set(CMAKE_CXX_FLAGS "-DBRPC_ENABLE_CPU_PROFILER")
        include_directories(${GPERFTOOLS_INCLUDE_DIR})
    else ()
        set (GPERFTOOLS_LIBRARIES "")
    endif ()
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CPP_FLAGS} -DGFLAGS_NS=${GFLAGS_NS} -DNDEBUG -g -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++11")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++11")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif()

if(CMAKE_VERSION VERSION_LESS "3.1.3")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
else()
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

## Micro-benchmark
if(MICRO_BM)
    message("-- Micro-benchmark enabled")
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DMICRO_BM")
    add_definitions(-DMICRO_BM)
endif()

## Snappy
if(LEVELDB_WITH_SNAPPY)
    find_library(SNAPPY_LIB NAMES snappy)
endif()

## glog
if (BRPC_WITH_GLOG)
    find_library(GLOG_LIBRARY NAMES glog libglog)
    if (NOT GLOG_LIBRARY)
        message(FATAL_ERROR "Fail to find glog")
    endif()
endif()

## OpenSSL
find_package(OpenSSL)
# find_package(OpenSSL 1.0.2 EXACT)

## Boost
find_package(Boost 1.58 COMPONENTS serialization)

## LevelDB
find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()
include_directories(${LEVELDB_INCLUDE_PATH})

## RocksDB
find_package(RocksDB 4.1 REQUIRED)

# Add ISA-L library
option(RS_RAFT "Enable RS-RAFT" OFF)
if (RS_RAFT)
    message("-- RS-Raft enabled")
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DRS_RAFT")
    add_definitions(-DRS_RAFT)

    find_library(ISAL_LIB NAMES isal)
endif()

# Add prometheus-cpp library
option(PROMETHEUS "Enable PROMETHEUS" OFF)
if (PROMETHEUS)
    message("-- prometheus-cpp enabled")
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DPROMETHEUS")
    find_package(prometheus-cpp REQUIRED)
    find_library(PROMETHEUS_CORE NAMES libprometheus-cpp-core.so)
    find_library(PROMETHEUS_PUSH NAMES libprometheus-cpp-push.so)
    find_library(PROMETHEUS_PULL NAMES libprometheus-cpp-pull.so)
endif()

## Set rpath
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

###############
### Project ###
###############

## kvstore executable
file(GLOB SERVER_SRC src/state_machine.cpp src/node.cpp src/config.cpp 
    src/metadata_manager.cpp src/closure.cpp src/multi_raft_service.cpp src/multi_raft_controller.cpp)
file(GLOB ADAPTER_SRC src/adapter/*.cpp)
file(GLOB SNAPSHOT_SRC src/snapshot/*.cpp)
add_executable(kvstore_server src/server.cpp ${PROTO_SRC} ${ADAPTER_SRC} ${SNAPSHOT_SRC} ${SERVER_SRC})

## include Librocksdb headers
target_include_directories(kvstore_server PUBLIC ${ROCKSDB_INCLUDE_DIR})

## link boost to kvstore
target_include_directories(kvstore_server PUBLIC ${Boost_INCLUDE_DIRS})

## link benchmark to kvstore
target_include_directories(kvstore_server PUBLIC ../../../src/braft/micro-benchmark)

set(DYNAMIC_LIB
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${GPERFTOOLS_LIBRARIES}
    ${BRAFT_LIB}
    ${BRPC_LIB}
    ${SNAPPY_LIB}
    rt
    ${GLOG_LIBRARY}
    ${OPENSSL_SSL_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${LEVELDB_LIB}
    ${ROCKSDB_LIBRARIES}
    Boost::serialization
    dl
    z
    stdc++fs
    )

if (PROMETHEUS)
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
        ${PROMETHEUS_CORE}
        ${PROMETHEUS_PUSH}
        ${PROMETHEUS_PULL}
    )
endif()

if (RS_RAFT)
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
        ${ISAL_LIB}
    )
endif()

target_link_libraries(kvstore_server
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")

## always copy scripts to build directory
## Source of this workaround: 
## https://stackoverflow.com/questions/13920072/how-to-always-run-command-when-building-regardless-of-any-dependency

add_custom_target(copy_scripts ALL
    DEPENDS
        custom_output
)

add_custom_command(
    OUTPUT custom_output
    COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_SOURCE_DIR}/*.sh
    ${CMAKE_CURRENT_BINARY_DIR}
    # DEPENDS
    #     copy_bm_scripts
)

# add_custom_command(
#     OUTPUT copy_bm_scripts
#     COMMAND ${CMAKE_COMMAND} -E copy
#     ${CMAKE_SOURCE_DIR}/benchmark/*
#     ${CMAKE_CURRENT_BINARY_DIR}/benchmark
# )

install(TARGETS kvstore_server
    DESTINATION bin
)

install(FILES ${PROJECT_BINARY_DIR}/hraftkv.pb.h
    DESTINATION include
)
