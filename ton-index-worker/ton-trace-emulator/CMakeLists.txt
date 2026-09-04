cmake_minimum_required(VERSION 3.16)

add_library(ton-trace-state STATIC
    src/TraceState.cpp
    src/TraceLifecycle.cpp
)
target_include_directories(ton-trace-state PUBLIC src)
target_compile_features(ton-trace-state PUBLIC cxx_std_20)

add_library(ton-confirmed-root-tracker STATIC
    src/ConfirmedRootTracker.cpp
)
target_include_directories(ton-confirmed-root-tracker
    PUBLIC ../external/ton
    PUBLIC ../external/ton/crypto
    PUBLIC src
)
target_link_libraries(ton-confirmed-root-tracker PUBLIC tdutils)
target_compile_features(ton-confirmed-root-tracker PUBLIC cxx_std_20)

add_library(ton-trace-assembler STATIC
    src/TraceAssembler.cpp
)
target_include_directories(ton-trace-assembler PUBLIC src)
target_compile_features(ton-trace-assembler PUBLIC cxx_std_20)
target_compile_definitions(ton-trace-assembler PRIVATE -DMSGPACK_USE_DEFINE_MAP)
target_link_libraries(
    ton-trace-assembler
    PUBLIC
        ton-trace-state
        ton-trace-emulator-core
        mch-emu-bridge
        msgpack-cxx
)

add_library(ton-trace-emulator-core STATIC
    src/TraceEmulator.cpp
    src/TraceUpdate.cpp
    src/ExternalMessageAdmission.cpp
    src/OverlayListener.cpp
    src/BlockEmulator.cpp
    src/TraceInterfaceDetector.cpp
    src/Measurement.cpp
)
target_include_directories(ton-trace-emulator-core
    PUBLIC ../external/ton
    PUBLIC src/
    PUBLIC tondb-scanner/src
)
target_link_directories(ton-trace-emulator-core
    PUBLIC ../external/ton
)

target_link_libraries(
    ton-trace-emulator-core
    PUBLIC
        tondb-scanner
        ton-observability
        overlay
        emulator_static
)
target_compile_features(ton-trace-emulator-core PRIVATE cxx_std_20)

add_executable(ton-trace-emulator
    src/main.cpp
    src/TraceScheduler.cpp
    src/TraceProcessor.cpp
    src/RedisMaterializer.cpp
    src/RedisListener.cpp
    src/Measurement.h
)
target_include_directories(ton-trace-emulator
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../external/redis-plus-plus/src
)

target_compile_features(ton-trace-emulator PRIVATE cxx_std_20)
target_compile_definitions(ton-trace-emulator PRIVATE -DMSGPACK_USE_DEFINE_MAP)
target_link_libraries(
    ton-trace-emulator
    ton-trace-emulator-core
    ton-trace-state
    ton-trace-assembler
    ton-confirmed-root-tracker
    mch-emu-bridge
    redis++_static
    msgpack-cxx
)
target_link_options(ton-trace-emulator PUBLIC -rdynamic)
install(TARGETS ton-trace-emulator RUNTIME DESTINATION bin)

add_executable(test-ton-trace-state
    test/TraceStateTest.cpp
    test/TraceAssemblerTest.cpp
    test/TraceUpdateTest.cpp
    test/TraceLifecycleTest.cpp
    test/ConfirmedRootTrackerTest.cpp
    test/OrderedResultBufferTest.cpp
    test/RedisMaterializerTest.cpp
    test/StreamingHintsTest.cpp
)
target_link_libraries(
    test-ton-trace-state
    PRIVATE
        ton-trace-state
        ton-trace-assembler
        ton-confirmed-root-tracker
        ton_crypto_core
        tdutils
        td-test-main
        msgpack-cxx
)
target_compile_features(test-ton-trace-state PRIVATE cxx_std_20)

# White-box coverage for TraceProcessor's translation-unit-local prepare helpers.
add_executable(test-ton-trace-processor
    test/TraceProcessorTest.cpp
    src/RedisMaterializer.cpp
)
target_include_directories(test-ton-trace-processor
    PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/../external/redis-plus-plus/src
)
target_compile_features(test-ton-trace-processor PRIVATE cxx_std_20)
target_compile_definitions(test-ton-trace-processor PRIVATE -DMSGPACK_USE_DEFINE_MAP)
target_link_libraries(
    test-ton-trace-processor
    PRIVATE
        ton-trace-emulator-core
        ton-trace-state
        ton-trace-assembler
        ton-confirmed-root-tracker
        mch-emu-bridge
        redis++_static
        msgpack-cxx
        td-test-main
)
