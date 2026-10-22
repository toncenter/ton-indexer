// Generates the Rust types of this module's output (probe.v1). It is only
// encoded: no views.
fn main() {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    buffa_build::Config::new()
        .files(&["proto/probe/v1/probe.proto"])
        .includes(&["proto"])
        .generate_views(false)
        .compile()
        .unwrap();
}
