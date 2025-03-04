cmake_minimum_required(VERSION 3.16)

add_executable(ton-integrity-checker
    src/main.cpp
    src/IntegrityChecker.cpp
)

target_compile_features(ton-integrity-checker PRIVATE cxx_std_20)
target_link_libraries(ton-integrity-checker tondb-scanner)

install(TARGETS ton-integrity-checker RUNTIME DESTINATION bin)
