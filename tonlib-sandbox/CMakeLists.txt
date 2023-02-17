cmake_minimum_required(VERSION 3.13)

add_executable(tonlib-sandbox src/main.cpp)

target_include_directories(tonlib-sandbox 
    PUBLIC external/ton
)

target_link_directories(tonlib-sandbox
    PUBLIC external/ton
)

target_link_libraries(tonlib-sandbox PRIVATE tonlib tdutils tdactor tl_api)
