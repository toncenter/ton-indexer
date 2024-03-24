cmake_minimum_required(VERSION 3.16)

add_executable(ton-index-postgres
    src/main.cpp
    src/InsertManagerPostgres.cpp
    src/IndexScheduler.cpp
)

target_include_directories(ton-index-postgres
    PUBLIC external/ton
    PUBLIC external/libpqxx
    PUBLIC tondb-scanner/src
    PUBLIC src/
)

target_link_directories(ton-index-postgres
    PUBLIC external/ton
    PUBLIC external/libpqxx
)

target_compile_features(ton-index-postgres PRIVATE cxx_std_17)
target_link_libraries(ton-index-postgres tondb-scanner overlay tdutils tdactor adnl tl_api dht ton_crypto
        catchain validatorsession validator-disk ton_validator validator-disk smc-envelope pqxx)

install(TARGETS ton-index-postgres RUNTIME DESTINATION bin)
