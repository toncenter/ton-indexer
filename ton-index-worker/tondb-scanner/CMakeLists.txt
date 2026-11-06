cmake_minimum_required(VERSION 3.16)

add_library(tondb-scanner STATIC
    src/InsertManager.cpp
    src/InsertManagerBase.cpp
    src/DbScanner.cpp
    src/DbEventListener.cpp
    src/DataParser.cpp
    src/TraceAssembler.cpp
    src/queue_state.cpp
    src/ElectorRoundResults.cpp
    src/ValidatorElection.cpp
    src/parse_token_data.cpp
    src/parse_contract_methods.cpp
    src/convert-utils.cpp
    src/tokens-tlb.cpp
    src/Statistics.cpp
    src/smc-interfaces/Tokens.cpp
    src/smc-interfaces/NftSale.cpp
    src/smc-interfaces/NftSaleV4.cpp
    src/smc-interfaces/execute-smc.cpp
    src/smc-interfaces/Multisig.cpp
    src/smc-interfaces/Multisig.h
    src/smc-interfaces/FetchAccountFromShard.cpp
    src/smc-interfaces/FetchAccountFromShard.h
    src/smc-interfaces/ParentAccountCache.cpp
    src/smc-interfaces/ParentCellCache.cpp
    src/smc-interfaces/Vesting.cpp
    src/smc-interfaces/NominatorPool.cpp
    src/smc-interfaces/Telemint.cpp
)

target_include_directories(tondb-scanner 
    PUBLIC src/
)

target_compile_features(tondb-scanner PRIVATE cxx_std_20)
target_link_libraries(tondb-scanner PUBLIC validator-db validator-common validator-disk ton_validator tddb smc-envelope msgpack-cxx emulator_static)

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

add_executable(test-elector-rounds EXCLUDE_FROM_ALL test/elector-round-results.cpp)
target_link_libraries(test-elector-rounds PRIVATE tondb-scanner)
add_test(NAME gramstat_elector_rounds COMMAND test-elector-rounds)

add_executable(test-memoized-parsers EXCLUDE_FROM_ALL test/memoized-parsers.cpp test/parent-cache.cpp)
target_link_libraries(test-memoized-parsers PRIVATE tondb-scanner)
target_compile_definitions(test-memoized-parsers PRIVATE
    ELECTOR_CACHE_FIXTURE="${CMAKE_CURRENT_SOURCE_DIR}/test/fixtures/elector-code.b64")
add_test(NAME gramstat_caches COMMAND test-memoized-parsers)
set_tests_properties(gramstat_caches PROPERTIES TIMEOUT 60)
