//! Generates the protobuf code of substreams-ton into `substreams-ton/src/pb`
//! from `ton-index-firehose/proto`, with buffa and a vendored protoc:
//!
//!     cargo run -p pbgen
//!
//! The code is checked in, so that the crate builds without protoc and
//! outside this repository. Run it again after a change of the .proto files.

use std::path::Path;

const FILES: &[&str] = &["sf/ton/type/v1/type.proto", "ton/state/v1/state.proto", "ton/tvm/v1/tvm.proto"];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let here = Path::new(env!("CARGO_MANIFEST_DIR"));
    let proto = here.join("../../../proto");
    let out = here.join("../../substreams-ton/src/pb");
    // Files of .proto that are gone must not stay behind.
    if out.exists() {
        std::fs::remove_dir_all(&out)?;
    }
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    buffa_build::Config::new()
        .files(&FILES.iter().map(|file| proto.join(file)).collect::<Vec<_>>())
        .includes(&[proto.clone(), protoc_bin_vendored::include_path()?])
        .out_dir(&out)
        .include_file("mod.rs")
        // Modules read a few fields of a large block: lazy views decode only those.
        .lazy_views(true)
        .compile()
}
