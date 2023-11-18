use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
    .file_descriptor_set_path(out_dir.join("map_reduce_descriptor.bin"))
    .compile(&["proto/map_reduce.proto"], &["proto"])
    .unwrap();
    Ok(())
}