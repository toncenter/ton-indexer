cmake_minimum_required(VERSION 3.16)

project(ton-index-cpp)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(PGTON "Enable adding the pgton subdirectory" OFF)
option(TON_USE_JEMALLOC "Use \"ON\" to enable JeMalloc." OFF)

# Find jemalloc
if (TON_USE_JEMALLOC)
    list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/external/ton/CMake")
    find_package(jemalloc REQUIRED)

    if (JEMALLOC_FOUND)
        include_directories(${JEMALLOC_INCLUDE_DIR})
        link_libraries(${JEMALLOC_LIBRARIES})
        add_compile_definitions(TON_USE_JEMALLOC)
    endif()
endif()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
add_subdirectory(external/ton EXCLUDE_FROM_ALL)
add_subdirectory(external/libpqxx EXCLUDE_FROM_ALL)

# The TON subtree already provides an lz4 target.  Tell clickhouse-cpp to use
# that configured library instead of adding its bundled target with the same
# plain name.
set(WITH_SYSTEM_LZ4 ON CACHE BOOL "Use configured LZ4 for clickhouse-cpp" FORCE)
set(WITH_SYSTEM_ABSEIL ON CACHE BOOL "Use configured Abseil for clickhouse-cpp" FORCE)
if (TARGET absl::int128)
    # clickhouse-cpp calls find_package(absl); route it to TON's in-tree Abseil
    # targets instead of importing a second system Abseil package.
    set(absl_DIR "${CMAKE_CURRENT_LIST_DIR}/cmake/absl" CACHE PATH "Use TON-provided Abseil targets for clickhouse-cpp" FORCE)
endif()
add_subdirectory(external/clickhouse-cpp EXCLUDE_FROM_ALL)
set(REDIS_PLUS_PLUS_BUILD_TEST OFF CACHE BOOL "disable test" FORCE)

# workaround to link static libhiredis
set(TON_INDEXER_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
find_library(TON_INDEXER_HIREDIS_STATIC_LIB hiredis REQUIRED)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${TON_INDEXER_CMAKE_FIND_LIBRARY_SUFFIXES})
unset(TON_INDEXER_CMAKE_FIND_LIBRARY_SUFFIXES)
set(HIREDIS_LIB "${TON_INDEXER_HIREDIS_STATIC_LIB}" CACHE FILEPATH "hiredis static library" FORCE)
set(REDIS_PLUS_PLUS_BUILD_SHARED OFF CACHE BOOL "disable shared redis++" FORCE)
add_subdirectory(external/redis-plus-plus EXCLUDE_FROM_ALL)
target_link_libraries(redis++_static PUBLIC ${TON_INDEXER_HIREDIS_STATIC_LIB})

set(MSGPACK_USE_BOOST OFF CACHE BOOL "disable boost" FORCE)
set(MSGPACK_USE_STD_VARIANT_ADAPTOR ON CACHE BOOL "enable std::variant" FORCE)
add_subdirectory(external/msgpack-c EXCLUDE_FROM_ALL)

function(add_ton_indexer_opentelemetry_cpp)
    set(BUILD_TESTING OFF)
    set(WITH_BENCHMARK OFF)
    set(WITH_EXAMPLES OFF)
    set(WITH_EXAMPLES_HTTP OFF)
    set(WITH_FUNC_TESTS OFF)
    set(WITH_OTLP_GRPC OFF)
    set(WITH_OTLP_HTTP ON)
    set(WITH_OTLP_FILE OFF)
    set(WITH_PROMETHEUS OFF)
    set(WITH_ZIPKIN OFF)
    set(WITH_ELASTICSEARCH OFF)
    set(OPENTELEMETRY_INSTALL OFF)
    set(Protobuf_USE_STATIC_LIBS ON CACHE BOOL "Prefer static protobuf libraries" FORCE)
    add_subdirectory(external/opentelemetry-cpp EXCLUDE_FROM_ALL)
endfunction()

add_ton_indexer_opentelemetry_cpp()

add_subdirectory(tondb-scanner)
add_subdirectory(ton-kvrocks-state)
add_subdirectory(ton-marker)
add_subdirectory(ton-observability)
add_subdirectory(ton-index-postgres)
add_subdirectory(ton-index-clickhouse)
add_subdirectory(ton-integrity-checker)
add_subdirectory(ton-smc-scanner)
add_subdirectory(ton-trace-emulator)
add_subdirectory(ton-trace-task-emulator)
add_subdirectory(celldb-migrate)
add_subdirectory(index-engine)

if (PGTON)
    message("Building pgton")
    add_subdirectory(pgton)
endif()
