// Generates the Rust types of this module's outputs and store values
// (traces.v1). traces.v1 refers to sf.ton.type.v1.Transaction: the type of
// substreams-ton. They are encoded and decoded whole: no views.
fn main() {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    buffa_build::Config::new()
        .files(&["proto/traces/v1/traces.proto"])
        .includes(&["../../../proto", "proto"])
        .extern_path(".sf.ton.type.v1", "::substreams_ton::types")
        .generate_views(false)
        .compile()
        .unwrap();
}
