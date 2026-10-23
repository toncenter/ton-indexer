// Generates the Rust types of this module's output (jettons.v1). It is only
// encoded: no views.
fn main() {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    buffa_build::Config::new()
        .files(&["proto/jettons/v1/jetton_transfers.proto"])
        .includes(&["proto"])
        .generate_views(false)
        .compile()
        .unwrap();
}
