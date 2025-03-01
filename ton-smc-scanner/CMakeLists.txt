cmake_minimum_required(VERSION 3.16)

add_executable(ton-smc-scanner
    src/main.cpp
    src/SmcScanner.cpp
    src/PostgreSQLInserter.cpp
)
target_include_directories(ton-smc-scanner
    PUBLIC src
)
target_compile_features(ton-smc-scanner PRIVATE cxx_std_20)
target_link_libraries(ton-smc-scanner tondb-scanner pqxx)

install(TARGETS ton-smc-scanner RUNTIME DESTINATION bin)
