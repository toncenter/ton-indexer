cmake_minimum_required(VERSION 3.13)

add_executable(tondb-scanner src/main.cpp)

target_include_directories(tondb-scanner 
    PUBLIC external/ton
)

target_link_directories(tondb-scanner
    PUBLIC external/ton
)

target_link_libraries(tondb-scanner overlay tdutils tdactor adnl tl_api dht
        catchain validatorsession validator-disk ton_validator validator-disk)
