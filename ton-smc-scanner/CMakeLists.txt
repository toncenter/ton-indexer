cmake_minimum_required(VERSION 3.16)

add_executable(ton-smc-scanner
    src/main.cpp
    src/SmcScanner.cpp
    src/PostgreSQLInserter.cpp
)

target_include_directories(ton-smc-scanner
    PUBLIC external/ton
    PUBLIC external/libpqxx
    PUBLIC src/
    PUBLIC tondb-scanner/src
)

target_link_directories(ton-smc-scanner
    PUBLIC external/ton
    PUBLIC external/libpqxx
)

target_compile_features(ton-smc-scanner PRIVATE cxx_std_20)
target_link_libraries(ton-smc-scanner tondb-scanner overlay tdutils tdactor adnl tl_api dht ton_crypto
        catchain validatorsession validator-disk ton_validator validator-disk smc-envelope pqxx)

install(TARGETS ton-smc-scanner RUNTIME DESTINATION bin)
