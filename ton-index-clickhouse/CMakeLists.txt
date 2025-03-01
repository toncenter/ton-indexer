cmake_minimum_required(VERSION 3.16)

add_executable(ton-index-clickhouse 
    src/main.cpp
    src/InsertManagerClickhouse.cpp
    src/IndexScheduler.cpp
)

target_compile_features(ton-index-clickhouse PRIVATE cxx_std_20)
target_link_libraries(ton-index-clickhouse tondb-scanner clickhouse-cpp-lib)

install(TARGETS ton-index-clickhouse RUNTIME DESTINATION bin)
