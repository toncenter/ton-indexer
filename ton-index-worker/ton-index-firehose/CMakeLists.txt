# The TON Firehose and substreams, C++ part: the reader that firecore runs on
# a node database, with the conversion of blocks and the protobuf of
# ../proto, and ton-tvm-executor, the backend of the ton_v1 extension.
# fireton (Go) and the substreams workspace (Rust) build with their own tools.
add_subdirectory(reader)
add_subdirectory(tvm-executor)
