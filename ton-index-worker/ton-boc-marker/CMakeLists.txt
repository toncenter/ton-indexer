cmake_minimum_required(VERSION 3.16)

# generate C++ from TL-B
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/auto/schemes.cpp ${CMAKE_CURRENT_BINARY_DIR}/auto/schemes.h
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/auto
    COMMAND tlbc 
    -o ${CMAKE_CURRENT_BINARY_DIR}/auto/schemes
    -n schemes
    -z
    ${CMAKE_CURRENT_SOURCE_DIR}/src/schemes/schemes.tlb
    COMMAND ${CMAKE_COMMAND} -E echo "Generated schemes.cpp and schemes.h from TLB"
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/schemes/schemes.tlb
    VERBATIM
)

add_executable(ton-boc-marker
    src/main.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/auto/schemes.h
    ${CMAKE_CURRENT_BINARY_DIR}/auto/schemes.cpp
    # src/BocMarker.cpp
)

target_compile_features(ton-boc-marker PRIVATE cxx_std_20)
target_link_libraries(ton-boc-marker tondb-scanner)

target_include_directories(ton-boc-marker PRIVATE 
    ${CMAKE_CURRENT_BINARY_DIR}/auto
    external/ton
)

install(TARGETS ton-boc-marker RUNTIME DESTINATION bin)
