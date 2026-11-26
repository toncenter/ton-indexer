cmake_minimum_required(VERSION 3.16)

enable_testing()

add_library(ton-dex-snapshots STATIC
    src/PoolMath.cpp
)

target_include_directories(ton-dex-snapshots
    PUBLIC
        src
)

target_compile_features(ton-dex-snapshots PRIVATE cxx_std_20)

target_link_libraries(ton-dex-snapshots
    PUBLIC
        ton_crypto_core
        tdutils
)

# Embed the schema so deployments need no separate SQL file.
set(DEX_SNAPSHOTS_SCHEMA ${CMAKE_CURRENT_SOURCE_DIR}/sql/schema.sql)
file(READ ${DEX_SNAPSHOTS_SCHEMA} DEX_SNAPSHOTS_SCHEMA_SQL)
configure_file(src/SnapshotWriterSchema.cpp.in ${CMAKE_CURRENT_BINARY_DIR}/SnapshotWriterSchema.cpp @ONLY)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${DEX_SNAPSHOTS_SCHEMA})

add_library(ton-dex-snapshots-writer STATIC
    src/PoolCache.cpp
    src/SnapshotWriter.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/SnapshotWriterSchema.cpp
)
target_compile_features(ton-dex-snapshots-writer PRIVATE cxx_std_20)
target_link_libraries(ton-dex-snapshots-writer PUBLIC ton-dex-snapshots tondb-scanner clickhouse-cpp-lib)

add_executable(ton-dex-snapshots-backfill backfill/main.cpp)
target_compile_features(ton-dex-snapshots-backfill PRIVATE cxx_std_20)
target_link_libraries(ton-dex-snapshots-backfill PRIVATE ton-dex-snapshots-writer tondb-scanner)
install(TARGETS ton-dex-snapshots-backfill RUNTIME DESTINATION bin)

add_executable(ton-dex-snapshots-test tests/dex-snapshots-test.cpp)
target_link_libraries(ton-dex-snapshots-test PRIVATE ton-dex-snapshots-writer tdutils tdactor tondb-scanner smc-envelope ton_block ton_crypto)
add_test(NAME dex_snapshots COMMAND ton-dex-snapshots-test)
