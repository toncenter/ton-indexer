cmake_minimum_required(VERSION 3.16)

add_library(ton-trace-emulator-core STATIC
    src/TraceEmulator.cpp
    src/OverlayListener.cpp
    src/BlockEmulator.cpp
    src/TraceInterfaceDetector.cpp
)
target_include_directories(ton-trace-emulator-core
    PUBLIC external/ton
    PUBLIC src/
    PUBLIC tondb-scanner/src
)
target_link_directories(ton-trace-emulator-core
    PUBLIC external/ton
)

target_link_libraries(ton-trace-emulator-core PUBLIC tondb-scanner overlay emulator)
target_compile_features(ton-trace-emulator-core PRIVATE cxx_std_20)

add_executable(ton-trace-emulator
    src/main.cpp
    src/TraceScheduler.cpp
    src/TraceInserter.cpp
    src/RedisListener.cpp
)
target_include_directories(ton-trace-emulator
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../external/redis-plus-plus/src
)
target_compile_features(ton-trace-emulator PRIVATE cxx_std_20)
target_compile_definitions(ton-trace-emulator PRIVATE -DMSGPACK_USE_DEFINE_MAP)
target_link_libraries(ton-trace-emulator ton-trace-emulator-core hiredis redis++_static msgpack-cxx)
target_link_options(ton-trace-emulator PUBLIC -rdynamic)
install(TARGETS ton-trace-emulator RUNTIME DESTINATION bin)
