cmake_minimum_required(VERSION 3.16)

project(ton-index-cpp)

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
add_subdirectory(external/clickhouse-cpp EXCLUDE_FROM_ALL)
set(REDIS_PLUS_PLUS_BUILD_TEST OFF CACHE BOOL "disable test" FORCE)
add_subdirectory(external/redis-plus-plus EXCLUDE_FROM_ALL)
set(MSGPACK_USE_BOOST OFF CACHE BOOL "disable boost" FORCE)
set(MSGPACK_USE_STD_VARIANT_ADAPTOR ON CACHE BOOL "enable std::variant" FORCE)
add_subdirectory(external/msgpack-c EXCLUDE_FROM_ALL)

add_subdirectory(tondb-scanner)
add_subdirectory(ton-index-postgres)
add_subdirectory(ton-index-clickhouse)
add_subdirectory(ton-integrity-checker)
add_subdirectory(ton-smc-scanner)
add_subdirectory(ton-trace-emulator)
add_subdirectory(ton-trace-task-emulator)
add_subdirectory(celldb-migrate)

if (PGTON)
    message("Building pgton")
    add_subdirectory(pgton)
endif()
