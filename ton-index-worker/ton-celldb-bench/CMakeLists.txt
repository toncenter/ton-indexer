cmake_minimum_required(VERSION 3.16)

add_executable(ton-celldb-bench
    src/main.cpp
    src/BenchmarkInsertManager.cpp
    src/BenchmarkStats.cpp
)

target_include_directories(ton-celldb-bench
    PUBLIC src
)

target_compile_features(ton-celldb-bench PRIVATE cxx_std_20)
target_link_libraries(ton-celldb-bench
    index-engine-core
    tondb-scanner
)

target_link_options(ton-celldb-bench PUBLIC -rdynamic)

install(TARGETS ton-celldb-bench RUNTIME DESTINATION bin)
