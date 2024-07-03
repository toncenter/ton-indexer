cmake_minimum_required(VERSION 3.16)

add_library(tondb-scanner STATIC
    src/InsertManager.cpp
    src/InsertManagerBase.cpp
    src/DbScanner.cpp
    src/DataParser.cpp
    src/TraceAssembler.cpp
    src/EventProcessor.cpp
    # src/EventProcessor2.cpp
    src/queue_state.cpp
    src/parse_token_data.cpp
    src/convert-utils.cpp
    src/tokens.cpp
    src/smc-interfaces/Tokens.cpp
    src/smc-interfaces/NftSale.cpp
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
        pqxx)

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
