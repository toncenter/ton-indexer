cmake_minimum_required(VERSION 3.16)

add_library(index-engine-core
        src/IndexScheduler.cpp
        src/BlockInterfacesDetector.cpp
)
target_include_directories(index-engine-core
        PUBLIC src/
)
target_compile_features(index-engine-core PRIVATE cxx_std_20)
target_link_libraries(index-engine-core
        tondb-scanner
        ton-observability)
