cmake_minimum_required(VERSION 3.16)

add_library(ton-kvrocks-state STATIC
    src/KvrocksState.cpp
)

target_include_directories(ton-kvrocks-state
    PUBLIC
        src
        ${CMAKE_CURRENT_BINARY_DIR}/../external/redis-plus-plus/src
)

target_compile_features(ton-kvrocks-state PRIVATE cxx_std_20)

target_link_libraries(ton-kvrocks-state
    PUBLIC
        tondb-scanner
        redis++_static
)
