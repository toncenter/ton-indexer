cmake_minimum_required(VERSION 3.16)

add_executable(ton-integrity-checker
    src/main.cpp
    src/IntegrityChecker.cpp
)

target_include_directories(ton-integrity-checker
    PUBLIC external/ton
    PUBLIC external/libpqxx
    PUBLIC src/
    PUBLIC tondb-scanner/src
)

target_link_directories(ton-integrity-checker
    PUBLIC external/ton
    PUBLIC external/libpqxx
)

target_compile_features(ton-integrity-checker PRIVATE cxx_std_17)
target_link_libraries(ton-integrity-checker tondb-scanner overlay tdutils tdactor adnl tl_api dht ton_crypto
        catchain validatorsession validator-disk ton_validator validator-disk smc-envelope pqxx)

install(TARGETS ton-integrity-checker RUNTIME DESTINATION bin)
