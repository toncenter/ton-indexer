// Generates the Rust types of the TON Firehose block (sf.ton.type.v1) and of
// the ton_v1 extension (ton.state.v1, ton.tvm.v1) from their .proto files.
fn main() {
    // These inputs and their transitive imports live outside this crate.
    println!("cargo:rerun-if-changed=../../proto");
    // A protoc of its own, so that nothing has to be installed for the build.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    let well_known = protoc_bin_vendored::include_path().unwrap();
    prost_build::compile_protos(
        &[
            "../../proto/sf/ton/type/v1/type.proto",
            "../../proto/ton/state/v1/state.proto",
            "../../proto/ton/tvm/v1/tvm.proto",
        ],
        &[std::path::Path::new("../../proto"), well_known.as_path()],
    )
    .unwrap();
}
