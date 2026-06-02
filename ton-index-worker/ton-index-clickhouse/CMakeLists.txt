cmake_minimum_required(VERSION 3.16)

add_executable(ton-index-clickhouse 
    src/main.cpp
    src/InsertManagerClickhouse.cpp
)
target_compile_features(ton-index-clickhouse PRIVATE cxx_std_20)
target_link_libraries(ton-index-clickhouse
        index-engine-core
        tondb-scanner
        ton-observability
        clickhouse-cpp-lib)
target_link_options(ton-index-clickhouse PUBLIC -rdynamic)


install(TARGETS ton-index-clickhouse RUNTIME DESTINATION bin)
