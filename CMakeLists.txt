set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

cmake_minimum_required(VERSION 3.16)

project(ton-indexer LANGUAGES CXX Go)

include(cmake/golang.cmake)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(ton-emulate-go)
add_subdirectory(ton-index-go)
add_subdirectory(ton-metadata-fetcher)
add_subdirectory(ton-index-worker)
add_subdirectory(ton-trace-ttl-tracker)
