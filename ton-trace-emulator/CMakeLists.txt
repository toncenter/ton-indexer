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
target_link_libraries(ton-trace-emulator-core tondb-scanner overlay ton_validator emulator)
target_compile_features(ton-trace-emulator-core PRIVATE cxx_std_17)

add_executable(ton-trace-emulator
    src/main.cpp
    src/TraceScheduler.cpp
    src/TraceInserter.cpp
)
target_include_directories(ton-trace-emulator
    PUBLIC external/ton
    PUBLIC src/
    PUBLIC tondb-scanner/src
    PUBLIC external/hiredis
    PUBLIC external/redis-plus-plus/src
    PUBLIC ${CMAKE_BINARY_DIR}/external/redis-plus-plus/src
    PUBLIC external/msgpack-c
)
target_link_directories(ton-trace-emulator
    PUBLIC external/ton
    PUBLIC external/hiredis
    PUBLIC external/redis-plus-plus
    PUBLIC external/msgpack-c
)
target_compile_features(ton-trace-emulator PRIVATE cxx_std_17)
target_link_libraries(ton-trace-emulator ton_validator ton-trace-emulator-core hiredis redis++ msgpack-cxx)
target_link_options(ton-trace-emulator PUBLIC -rdynamic)
install(TARGETS ton-trace-emulator RUNTIME DESTINATION bin)
