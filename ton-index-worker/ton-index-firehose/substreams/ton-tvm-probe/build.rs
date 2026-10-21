// Generates the Rust types of this module's output (probe.v1).
fn main() {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    prost_build::compile_protos(&["proto/probe/v1/probe.proto"], &["proto"]).unwrap();
}
