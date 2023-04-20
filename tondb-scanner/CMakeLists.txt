cmake_minimum_required(VERSION 3.21)

add_executable(tondb-scanner 
    src/main.cpp
    src/InsertManagerPostgres.cpp
    src/DbScanner.cpp
    src/DataParser.cpp
)

target_include_directories(tondb-scanner 
    PUBLIC external/ton
    PUBLIC external/libpqxx
    PUBLIC src/
)

target_link_directories(tondb-scanner
    PUBLIC external/ton
    PUBLIC external/libpqxx
)
target_compile_features(tondb-scanner PRIVATE cxx_std_17)
target_link_libraries(tondb-scanner overlay tdutils tdactor adnl tl_api dht
        catchain validatorsession validator-disk ton_validator validator-disk 
        pqxx pq)
