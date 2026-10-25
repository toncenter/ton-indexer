cmake_minimum_required(VERSION 3.16)

add_executable(ton-index-postgres
    src/main.cpp
    src/FailoverWatchdog.cpp
    src/InsertManagerPostgres.cpp
    src/KvrocksClient.cpp
    src/KvrocksProgress.cpp
    src/PartitionManagerPostgres.cpp
    src/GramstatPostgres.cpp
)

target_include_directories(ton-index-postgres
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../external/redis-plus-plus/src
)
target_compile_features(ton-index-postgres PRIVATE cxx_std_20)
target_link_libraries(
    ton-index-postgres
    index-engine-core
    tondb-scanner
    ton-kvrocks-state
    ton-observability
    mch-classify
    pqxx
    redis++_static
)
target_link_options(ton-index-postgres PUBLIC -rdynamic)

install(TARGETS ton-index-postgres RUNTIME DESTINATION bin)

add_executable(ton-index-postgres-migrate
    src/migrate.cpp
)

target_compile_features(ton-index-postgres-migrate PRIVATE cxx_std_20)
target_link_libraries(ton-index-postgres-migrate pqxx tdutils)
target_link_options(ton-index-postgres-migrate PUBLIC -rdynamic)

install(TARGETS ton-index-postgres-migrate RUNTIME DESTINATION bin)

add_executable(test-gramstat-postgres EXCLUDE_FROM_ALL test/total_supply.cpp src/GramstatPostgres.cpp)
target_include_directories(test-gramstat-postgres PRIVATE src)
target_link_libraries(test-gramstat-postgres PRIVATE tondb-scanner pqxx)
add_dependencies(test-gramstat-postgres ton-index-postgres-migrate)

find_package(Python3 COMPONENTS Interpreter QUIET)
find_program(GRAMSTAT_PG_CONFIG pg_config)
if (GRAMSTAT_PG_CONFIG)
    execute_process(COMMAND ${GRAMSTAT_PG_CONFIG} --bindir
        OUTPUT_VARIABLE GRAMSTAT_PG_BINDIR OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()
find_program(GRAMSTAT_INITDB initdb HINTS ${GRAMSTAT_PG_BINDIR})
find_program(GRAMSTAT_PG_CTL pg_ctl HINTS ${GRAMSTAT_PG_BINDIR})
if (Python3_Interpreter_FOUND AND GRAMSTAT_INITDB AND GRAMSTAT_PG_CTL)
    add_test(NAME gramstat_postgres COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/test/total_supply_pg.py
        $<TARGET_FILE:test-gramstat-postgres> ${GRAMSTAT_INITDB} ${GRAMSTAT_PG_CTL}
        $<TARGET_FILE:ton-index-postgres-migrate>)
    set_tests_properties(gramstat_postgres PROPERTIES SKIP_RETURN_CODE 77)
endif()
