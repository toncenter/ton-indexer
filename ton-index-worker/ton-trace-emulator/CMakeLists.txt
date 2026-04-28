cmake_minimum_required(VERSION 3.16)

add_library(ton-trace-emulator-core STATIC
    src/TraceEmulator.cpp
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

target_link_libraries(ton-trace-emulator-core PUBLIC tondb-scanner overlay emulator_static)
target_compile_features(ton-trace-emulator-core PRIVATE cxx_std_20)

add_executable(ton-trace-emulator
    src/main.cpp
    src/DbEventListener.cpp
    src/TraceScheduler.cpp
    src/InvalidatedTraceTracker.cpp
    src/TraceInserter.cpp
    src/RedisListener.cpp
    src/Measurement.h
    src/Measurement.cpp
)
target_include_directories(ton-trace-emulator
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../external/redis-plus-plus/src
)

set(TRACE_EMULATOR_LINK_LIBS hiredis)
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    message("-- Skipping link hiredis for Darwin")
    set(TRACE_EMULATOR_LINK_LIBS "")
endif()

target_compile_features(ton-trace-emulator PRIVATE cxx_std_20)
target_compile_definitions(ton-trace-emulator PRIVATE -DMSGPACK_USE_DEFINE_MAP)
target_link_libraries(ton-trace-emulator ton-trace-emulator-core redis++_static msgpack-cxx ${TRACE_EMULATOR_LINK_LIBS})
target_link_options(ton-trace-emulator PUBLIC -rdynamic)
install(TARGETS ton-trace-emulator RUNTIME DESTINATION bin)
