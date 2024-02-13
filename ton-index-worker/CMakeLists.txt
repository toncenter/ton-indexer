cmake_minimum_required(VERSION 3.21)
add_executable(ton-index-worker 
    src/main.cpp
    src/InsertManager.cpp
    src/InsertManagerBase.cpp
    src/InsertManagerPostgres.cpp
    # src/InsertManagerClickhouse.cpp
    src/DbScanner.cpp
    src/DataParser.cpp
    src/parse_token_data.cpp
    # src/EventProcessor.cpp
    src/IndexScheduler.cpp
    src/convert-utils.cpp
    src/tokens.cpp
)

target_include_directories(ton-index-worker 
    PUBLIC external/ton
    PUBLIC external/libpqxx
    # PUBLIC external/clickhouse-cpp
    PUBLIC src/
)

target_link_directories(ton-index-worker
    PUBLIC external/ton
    PUBLIC external/libpqxx
    # PUBLIC external/clickhouse-cpp
)
target_compile_features(ton-index-worker PRIVATE cxx_std_17)
target_link_libraries(ton-index-worker overlay tdutils tdactor adnl tl_api dht ton_crypto
        catchain validatorsession validator-disk ton_validator validator-disk smc-envelope
        pqxx) # clickhouse-cpp-lib)

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

add_custom_target(tlb_generate_tokens_2 DEPENDS ${TLB_TOKENS})
add_dependencies(ton-index-worker tlb_generate_tokens_2)
