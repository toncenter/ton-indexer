cmake_minimum_required(VERSION 3.16)

add_library(index-engine-core
        src/IndexScheduler.cpp
        src/BlockInterfacesDetector.cpp
        src/TotalSupply.cpp
        # The read-only API is in the full validator target, whereas this
        # indexer links validator-disk. Build the upstream calculator TU only.
        ../external/ton/validator/global-balance-calculator.cpp
)
target_include_directories(index-engine-core
        PUBLIC src/
)
target_compile_features(index-engine-core PRIVATE cxx_std_20)
target_link_libraries(index-engine-core
        tondb-scanner
        ton-observability
        mch-classify)
