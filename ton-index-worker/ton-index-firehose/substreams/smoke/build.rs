// Generates the Rust types of this module's outputs (smoke.v1). They are
// encoded and decoded whole: no views.
fn main() {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    buffa_build::Config::new()
        .files(&["proto/smoke/v1/smoke.proto"])
        .includes(&["proto"])
        .generate_views(false)
        .compile()
        .unwrap();
}
