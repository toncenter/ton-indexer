cmake_minimum_required(VERSION 3.16)

add_executable(ton-index-clickhouse 
    src/main.cpp
    src/InsertManagerClickhouse.cpp
    src/IndexScheduler.cpp
)

target_include_directories(ton-index-clickhouse 
    PUBLIC external/ton
    PUBLIC external/clickhouse-cpp
    PUBLIC tondb-scanner/src
    PUBLIC src/
)

target_link_directories(ton-index-clickhouse
    PUBLIC external/ton
    PUBLIC external/clickhouse-cpp
)
target_compile_features(ton-index-clickhouse PRIVATE cxx_std_17)
target_link_libraries(ton-index-clickhouse tondb-scanner overlay tdutils tdactor adnl tl_api dht ton_crypto
        catchain validatorsession validator-disk ton_validator validator-disk smc-envelope
        clickhouse-cpp-lib)

install(TARGETS ton-index-clickhouse RUNTIME DESTINATION bin)
