cmake_minimum_required(VERSION 3.21)

add_executable(tondb-scanner 
    src/main.cpp
    src/InsertManager.cpp
    src/InsertManagerPostgres.cpp
    src/DbScanner.cpp
    src/DataParser.cpp
    src/parse_token_data.cpp
    src/EventProcessor.cpp
    src/convert-utils.cpp
    src/tokens.cpp
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
        catchain validatorsession validator-disk ton_validator validator-disk smc-envelope
        pqxx pq)

set(TLB_TOKENS
  ${CMAKE_CURRENT_SOURCE_DIR}/src/tokens.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/tokens.h
)

add_custom_command(
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src
    COMMAND tlbc -o tokens -n tokens::gen -z tlb/tokens.tlb
    COMMENT "Generate tokes tlb source files"
    OUTPUT ${TLB_TOKENS}
    DEPENDS tlbc src/tlb/tokens.tlb
)

  add_custom_target(tlb_generate_tokens DEPENDS ${TLB_TOKENS})
  add_dependencies(tondb-scanner tlb_generate_tokens)

add_executable(tondb-scanner-test
    ../external/ton/test/test-td-main.cpp
    src/InsertManager.cpp
    src/InsertManagerPostgres.cpp
    src/DbScanner.cpp
    src/DataParser.cpp
    src/parse_token_data.cpp
    src/EventProcessor.cpp
    src/convert-utils.cpp
    src/tokens.cpp
    test/tests.cpp)

target_include_directories(tondb-scanner-test
    PUBLIC external/ton
    PUBLIC external/libpqxx
    PUBLIC src/
)
target_link_directories(tondb-scanner-test
    PUBLIC external/ton
    PUBLIC external/libpqxx
)
target_compile_features(tondb-scanner-test PRIVATE cxx_std_17)
target_link_libraries(tondb-scanner-test overlay tdutils tdactor adnl tl_api dht
        catchain validatorsession validator-disk ton_validator validator-disk smc-envelope
        pqxx pq)