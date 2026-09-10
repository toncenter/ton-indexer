cmake_minimum_required(VERSION 3.16)

add_executable(ton-smc-scanner
    src/main.cpp
    src/SmcScanner.cpp
    src/PostgreSQLInserter.cpp
    src/ShardBatchScanner.cpp
)
target_include_directories(ton-smc-scanner
    PUBLIC src
)
target_compile_features(ton-smc-scanner PRIVATE cxx_std_20)
# pqxx first: redis++_static pulls Homebrew's include prefix in transitively,
# and a Homebrew libpqxx there would otherwise shadow the vendored headers.
target_link_libraries(ton-smc-scanner pqxx tondb-scanner ton-kvrocks-state)

set(INTERFACES_DETECTOR_TYPES "" CACHE STRING "Semicolon-separated list of detectors to instantiate")
string(REPLACE ";" "," DETECTORS_COMMA "${INTERFACES_DETECTOR_TYPES}")

if(DETECTORS_COMMA)
    target_compile_definitions(ton-smc-scanner PRIVATE INTERFACES_DETECTOR_TYPES=${DETECTORS_COMMA})
endif(DETECTORS_COMMA)

install(TARGETS ton-smc-scanner RUNTIME DESTINATION bin)
