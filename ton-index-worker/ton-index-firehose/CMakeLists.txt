cmake_minimum_required(VERSION 3.16)

find_package(Protobuf REQUIRED)
find_library(ZSTD_LIBRARY NAMES libzstd.a zstd REQUIRED)

# protoc 3.12 needs the experimental flag for proto3 optional fields,
# which protobuf_generate_cpp cannot pass.
set(FIREHOSE_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(FIREHOSE_PROTO_OUT ${CMAKE_CURRENT_BINARY_DIR}/proto)
set(FIREHOSE_PROTO_SRCS)
foreach(proto sf/ton/type/v1/type.proto sf/bstream/v1/bstream.proto)
  string(REGEX REPLACE "\\.proto$" "" proto_base ${proto})
  list(APPEND FIREHOSE_PROTO_SRCS ${FIREHOSE_PROTO_OUT}/${proto_base}.pb.cc)
  add_custom_command(
    OUTPUT ${FIREHOSE_PROTO_OUT}/${proto_base}.pb.cc ${FIREHOSE_PROTO_OUT}/${proto_base}.pb.h
    COMMAND ${CMAKE_COMMAND} -E make_directory ${FIREHOSE_PROTO_OUT}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE} --experimental_allow_proto3_optional
            --proto_path=${FIREHOSE_PROTO_DIR} --proto_path=${Protobuf_INCLUDE_DIRS}
            --cpp_out=${FIREHOSE_PROTO_OUT} ${proto}
    DEPENDS ${FIREHOSE_PROTO_DIR}/${proto}
    COMMENT "Generating C++ for ${proto}")
endforeach()

# The messages, also for ton-tvm-executor, whose protos import type.proto.
add_library(ton-firehose-proto STATIC ${FIREHOSE_PROTO_SRCS})
target_include_directories(ton-firehose-proto PUBLIC ${FIREHOSE_PROTO_OUT} ${Protobuf_INCLUDE_DIRS})
target_link_libraries(ton-firehose-proto PUBLIC ${Protobuf_LIBRARIES})

# Cells to messages, also for ton-tvm-executor.
add_library(ton-firehose-convert STATIC
    src/FirehoseBlock.cpp
    src/CellCache.cpp
)
target_include_directories(ton-firehose-convert PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_features(ton-firehose-convert PUBLIC cxx_std_20)
target_link_libraries(ton-firehose-convert PUBLIC tondb-scanner ton-firehose-proto)

add_executable(ton-index-firehose
    src/main.cpp
    src/InsertManagerFirehose.cpp
    src/FirehoseOutput.cpp
)
target_compile_features(ton-index-firehose PRIVATE cxx_std_20)
target_link_libraries(ton-index-firehose
        index-engine-core
        ton-firehose-convert
        ton-observability
        ${ZSTD_LIBRARY})
target_link_options(ton-index-firehose PUBLIC -rdynamic)

install(TARGETS ton-index-firehose RUNTIME DESTINATION bin)
