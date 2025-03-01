cmake_minimum_required(VERSION 3.16)

add_executable(celldb-migrate
    src/main.cpp
)
target_include_directories(celldb-migrate
    PUBLIC src
)
target_compile_features(celldb-migrate PRIVATE cxx_std_20)
target_link_libraries(celldb-migrate tondb-scanner rocksdb)

install(TARGETS celldb-migrate RUNTIME DESTINATION bin)
