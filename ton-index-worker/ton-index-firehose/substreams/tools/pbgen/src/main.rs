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
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    generate(&proto, &out)
}

fn generate(proto: &Path, out: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Generate beside the destination, so a failed compile leaves it intact
    // and replacing it does not cross filesystems.
    let temporary = tempfile::tempdir_in(out.parent().ok_or("output has no parent directory")?)?;
    let generated = temporary.path().join("generated");
    buffa_build::Config::new()
        .files(&FILES.iter().map(|file| proto.join(file)).collect::<Vec<_>>())
        .includes(&[proto.to_path_buf(), protoc_bin_vendored::include_path()?])
        .out_dir(&generated)
        .include_file("mod.rs")
        // Modules read a few fields of a large block: lazy views decode only those.
        .lazy_views(true)
        .compile()?;

    // Replacing the directory also removes bindings of deleted .proto files.
    let previous = temporary.path().join("previous");
    if out.exists() {
        std::fs::rename(out, &previous)?;
    }
    if let Err(error) = std::fs::rename(&generated, out) {
        if previous.exists() {
            if let Err(restore) = std::fs::rename(&previous, out) {
                let saved = temporary.keep();
                return Err(
                    format!("{error}; restoring bindings failed: {restore}; backup: {}", saved.display()).into()
                );
            }
        }
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_generation_preserves_bindings_and_success_removes_stale_files() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
        let root = tempfile::tempdir().unwrap();
        let proto = root.path().join("proto");
        let out = root.path().join("pb");
        std::fs::create_dir(&out).unwrap();
        let previous = out.join("old.rs");
        std::fs::write(&previous, "old bindings").unwrap();
        for (i, file) in FILES.iter().enumerate() {
            let path = proto.join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, format!("syntax = \"proto3\"; package test.v1; message Message{i} {{}}")).unwrap();
        }
        let file = proto.join(FILES[0]);
        let valid = std::fs::read(&file).unwrap();
        std::fs::write(&file, "invalid protobuf").unwrap();
        assert!(generate(&proto, &out).is_err());
        assert_eq!(std::fs::read_to_string(&previous).unwrap(), "old bindings");

        std::fs::write(file, valid).unwrap();
        generate(&proto, &out).unwrap();
        assert!(out.join("mod.rs").exists());
        assert!(!previous.exists());
    }
}
