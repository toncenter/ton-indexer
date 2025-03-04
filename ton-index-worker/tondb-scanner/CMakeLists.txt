cmake_minimum_required(VERSION 3.16)

add_library(tondb-scanner STATIC
    src/InsertManager.cpp
    src/InsertManagerBase.cpp
    src/DbScanner.cpp
    src/DataParser.cpp
    src/TraceAssembler.cpp
    src/EventProcessor.cpp
    src/queue_state.cpp
    src/parse_token_data.cpp
    src/convert-utils.cpp
    src/tokens-tlb.cpp
    src/Statistics.cpp
    src/smc-interfaces/Tokens.cpp
    src/smc-interfaces/NftSale.cpp
    src/smc-interfaces/execute-smc.cpp
)

target_include_directories(tondb-scanner 
    PUBLIC src/
)

target_compile_features(tondb-scanner PRIVATE cxx_std_20)
target_link_libraries(tondb-scanner PUBLIC validator-disk ton_validator tddb smc-envelope msgpack-cxx)

set(TLB_TOKENS
  ${CMAKE_CURRENT_SOURCE_DIR}/src/tokens-tlb.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/tokens-tlb.h
)

add_custom_command(
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src
    COMMAND tlbc -o tokens-tlb -n tokens::gen -z tlb/tokens.tlb
    COMMENT "Generate tokes tlb source files"
    OUTPUT ${TLB_TOKENS}
    DEPENDS tlbc src/tlb/tokens.tlb
)

add_custom_target(tlb_generate_tokens DEPENDS ${TLB_TOKENS})
add_dependencies(tondb-scanner tlb_generate_tokens)

set(TONDB_SCANNER_TEST_SOURCE
  ${CMAKE_CURRENT_SOURCE_DIR}/test/tests.cpp
  PARENT_SCOPE
)

add_executable(test-tondb test/tests.cpp ${TONDB_SCANNER_TEST_SOURCE})
target_include_directories(test-tondb
    PUBLIC external/ton
    PUBLIC src/
)

target_link_libraries(test-tondb PRIVATE tdutils tdactor smc-envelope tondb-scanner ton_block)
add_dependencies(test-tondb test-tdutils)