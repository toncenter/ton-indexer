// Generates the Rust types of the TON Firehose block (sf.ton.type.v1) and of
// this module's outputs (traces.v1) from their .proto files.
fn main() {
    println!("cargo:rerun-if-changed=../../proto");
    println!("cargo:rerun-if-changed=proto");
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    let mut config = prost_build::Config::new();
    // traces.v1 refers to sf.ton.type.v1.Transaction: use the generated type.
    config.extern_path(".sf.ton.type.v1", "crate::pb::ton");
    config
        .compile_protos(&["proto/traces/v1/traces.proto"], &["../../proto", "proto"])
        .unwrap();
    prost_build::compile_protos(&["../../proto/sf/ton/type/v1/type.proto"], &["../../proto"]).unwrap();
}
