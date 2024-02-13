cmake_minimum_required(VERSION 3.21)

project(ton-index-cpp)

add_subdirectory(external/ton EXCLUDE_FROM_ALL)
add_subdirectory(external/libpqxx EXCLUDE_FROM_ALL)
# add_subdirectory(external/clickhouse-cpp EXCLUDE_FROM_ALL)

add_subdirectory(tondb-scanner)
add_subdirectory(ton-index-worker)
