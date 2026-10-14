fn main() {
    println!("cargo:rerun-if-env-changed=RPCSTACK_QUEUE_SOURCE");
    if std::env::var_os("CARGO_FEATURE_ORF").is_some() {
        assert!(
            std::env::var_os("RPCSTACK_QUEUE_SOURCE").is_some(),
            "orf requires an external queue; build through examples/tailclipper/cargo.sh"
        );
        tonic_build::configure()
            .default_hooks_path("crate::arrival_price::PaperHooks")
            .compile(&["proto/arrival_price.proto"], &["proto"])
            .expect("compile arrival/price composition service");
    }
}
