#include <iostream>
#include <pybind11/pybind11.h>

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

namespace py = pybind11;


void hello() {
    std::cout << "Hello, World!" << std::endl;
}


PYBIND11_MODULE(pymodule, m) {
    m.def("hello", &hello);

#ifdef VERSION_INFO
    m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
    m.attr("__version__") = "dev";
#endif
}
