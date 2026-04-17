cmake_minimum_required(VERSION 3.16)

add_library(ton-observability STATIC
    src/Otel.cpp
)

target_include_directories(ton-observability
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(
    ton-observability
    PUBLIC
        opentelemetry-cpp::trace
        opentelemetry-cpp::otlp_http_exporter
)

target_compile_features(ton-observability PRIVATE cxx_std_20)
