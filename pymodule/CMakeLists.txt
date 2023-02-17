cmake_minimum_required(VERSION 3.13)

pybind11_add_module(pymodule src/main.cpp)
target_compile_definitions(pymodule
                           PRIVATE VERSION_INFO=${EXAMPLE_VERSION_INFO})
