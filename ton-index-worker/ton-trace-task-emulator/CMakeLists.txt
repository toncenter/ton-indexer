cmake_minimum_required(VERSION 3.16)

add_executable(ton-trace-task-emulator
    src/main.cpp
    src/TraceTaskScheduler.cpp
    src/TaskResultInserter.cpp
    src/RedisListener.cpp
    src/TonConnectProcessor.cpp
)
target_include_directories(ton-trace-task-emulator
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../external/redis-plus-plus/src
)

set(TRACE_TASK_EMULATOR_LINK_LIBS hiredis)
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    message("-- Skipping link hiredis for Darwin")
    set(TRACE_TASK_EMULATOR_LINK_LIBS "")
endif()

target_compile_features(ton-trace-task-emulator PRIVATE cxx_std_20)
target_link_libraries(ton-trace-task-emulator ton-trace-emulator-core redis++_static msgpack-cxx ${TRACE_TASK_EMULATOR_LINK_LIBS})
target_link_options(ton-trace-task-emulator PUBLIC -rdynamic)
target_compile_definitions(ton-trace-task-emulator PRIVATE -DMSGPACK_USE_DEFINE_MAP)
install(TARGETS ton-trace-task-emulator RUNTIME DESTINATION bin)
