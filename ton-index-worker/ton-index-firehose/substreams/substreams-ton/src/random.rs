//! The randomness of getrandom on wasm32, for the hash maps of rston.

// rston hashes with ahash, whose seeds come from getrandom, which has no
// source of randomness on wasm32-unknown-unknown and which a module must not
// depend on anyway: a constant keeps the seeds, and the order of hash maps,
// the same on every run. A module selects this backend with the cfg
// getrandom_backend="custom" (see the documentation of the crate).
#[cfg(target_arch = "wasm32")]
#[no_mangle]
unsafe extern "Rust" fn __getrandom_v03_custom(dest: *mut u8, len: usize) -> Result<(), getrandom::Error> {
    core::ptr::write_bytes(dest, 0x5a, len);
    Ok(())
}
