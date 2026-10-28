use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rustc-check-cfg=cfg(rpcstack_external_queue)");
    println!("cargo:rerun-if-env-changed=RPCSTACK_QUEUE_SOURCE");
    let declaration = match env::var_os("RPCSTACK_QUEUE_SOURCE") {
        None => String::new(),
        // Host build dependencies may compile this crate without sched_custom
        // in the same Cargo invocation. Only opt-in targets select the source.
        Some(_) if env::var_os("CARGO_FEATURE_SCHED_CUSTOM").is_none() => String::new(),
        Some(source) => {
            let path = PathBuf::from(source);
            assert!(
                path.is_absolute(),
                "RPCSTACK_QUEUE_SOURCE must be an absolute source path"
            );
            let path = path
                .canonicalize()
                .expect("RPCSTACK_QUEUE_SOURCE must name an existing Rust source file");
            assert!(path.is_file(), "RPCSTACK_QUEUE_SOURCE must name a file");
            println!("cargo:rerun-if-changed={}", path.display());
            println!("cargo:rustc-cfg=rpcstack_external_queue");
            format!("#[path = {:?}] pub mod external;\n", path)
        }
    };
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("external_queue.rs"),
        declaration,
    )
    .expect("write external queue module declaration");
}
