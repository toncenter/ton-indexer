cmake_minimum_required(VERSION 3.16)

add_executable(sandbox-cpp
    src/main.cpp
    src/Scheduler.cpp
    src/DbReader.cpp
)

target_include_directories(sandbox-cpp 
    PUBLIC external/ton
    PUBLIC tondb-scanner/src
    PUBLIC src/
)

target_link_directories(sandbox-cpp 
    PUBLIC external/ton
)
target_compile_features(sandbox-cpp PRIVATE cxx_std_17)
target_link_libraries(sandbox-cpp tondb-scanner overlay tdutils tdactor adnl tl_api dht ton_crypto
        catchain validatorsession validator-disk ton_validator validator-disk smc-envelope)
