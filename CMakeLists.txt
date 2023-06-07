cmake_minimum_required(VERSION 3.13)

project(ton-index-cpp)

add_subdirectory(external/ton EXCLUDE_FROM_ALL)
add_subdirectory(external/libpqxx EXCLUDE_FROM_ALL)

add_subdirectory(tondb-scanner)
