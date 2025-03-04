cmake_minimum_required(VERSION 3.16)

add_executable(ton-index-postgres
    src/main.cpp
    src/InsertManagerPostgres.cpp
    src/IndexScheduler.cpp
)

target_compile_features(ton-index-postgres PRIVATE cxx_std_20)
target_link_libraries(ton-index-postgres tondb-scanner pqxx)
target_link_options(ton-index-postgres PUBLIC -rdynamic)

install(TARGETS ton-index-postgres RUNTIME DESTINATION bin)
