cmake_minimum_required(VERSION 3.16)

# generate C++ from TL-B
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/auto/schemes.cpp ${CMAKE_CURRENT_BINARY_DIR}/auto/schemes.h
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/auto
    COMMAND $<TARGET_FILE:tlbc>
    -o ${CMAKE_CURRENT_BINARY_DIR}/auto/schemes
    -n schemes
    -z
    ${CMAKE_CURRENT_SOURCE_DIR}/src/schemes/schemes.tlb
    COMMAND ${CMAKE_COMMAND} -E echo "Generated schemes.cpp and schemes.h from TLB"
    DEPENDS tlbc ${CMAKE_CURRENT_SOURCE_DIR}/src/schemes/schemes.tlb
    VERBATIM
)

# core library
add_library(ton-marker-core STATIC
    src/logic.cpp
    src/interfaces.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/auto/schemes.h
    ${CMAKE_CURRENT_BINARY_DIR}/auto/schemes.cpp
)

target_compile_features(ton-marker-core PUBLIC cxx_std_20)
target_link_libraries(ton-marker-core PUBLIC tondb-scanner)
target_include_directories(ton-marker-core PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}/auto
    external/ton
)

# c-api library for go
add_library(ton-marker SHARED
    src/wrapper.cpp
)

target_compile_features(ton-marker PRIVATE cxx_std_20)
target_link_libraries(ton-marker PRIVATE ton-marker-core)

# cli executable
add_executable(ton-marker-cli
    src/main.cpp
)

target_compile_features(ton-marker-cli PRIVATE cxx_std_20)
target_link_libraries(ton-marker-cli PRIVATE ton-marker-core)

# install targets
install(TARGETS ton-marker-core ton-marker ton-marker-cli
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(FILES 
    src/logic.h
    src/wrapper.h
    DESTINATION include/ton-marker
)
