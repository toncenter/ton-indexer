cmake_minimum_required(VERSION 3.16)

add_executable(ton-index-clickhouse 
    src/main.cpp
    src/InsertManagerClickhouse.cpp
    src/GramstatStorage.cpp
    src/TokenStatesStorage.cpp
    src/ElectorSnapshotsStorage.cpp
)
target_compile_features(ton-index-clickhouse PRIVATE cxx_std_20)
target_link_libraries(ton-index-clickhouse
        index-engine-core
        tondb-scanner
        ton-observability
        clickhouse-cpp-lib)
target_link_options(ton-index-clickhouse PUBLIC -rdynamic)


install(TARGETS ton-index-clickhouse RUNTIME DESTINATION bin)

add_executable(test-gramstat EXCLUDE_FROM_ALL test/total_supply.cpp src/GramstatStorage.cpp src/TokenStatesStorage.cpp src/ElectorSnapshotsStorage.cpp)
target_include_directories(test-gramstat PRIVATE src)
target_link_libraries(test-gramstat PRIVATE index-engine-core clickhouse-cpp-lib)
add_test(NAME gramstat_native COMMAND test-gramstat)

find_program(GRAMSTAT_CLICKHOUSE_EXECUTABLE clickhouse)
find_package(Python3 COMPONENTS Interpreter QUIET)
if (GRAMSTAT_CLICKHOUSE_EXECUTABLE AND Python3_Interpreter_FOUND)
    add_test(NAME gramstat_sql COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/test/total_supply_sql.py
        $<TARGET_FILE:test-gramstat> ${GRAMSTAT_CLICKHOUSE_EXECUTABLE})
endif()
