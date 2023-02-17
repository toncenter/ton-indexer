cmake_minimum_required(VERSION 3.13)

project(ton-index-sanbox)

add_subdirectory(external/pybind11 EXCLUDE_FROM_ALL)
add_subdirectory(external/ton EXCLUDE_FROM_ALL)

add_subdirectory(tonlib-sandbox)
add_subdirectory(pymodule)
add_subdirectory(tondb-scanner)
