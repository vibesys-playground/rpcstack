fn main() {
    tonic_build::configure()
        .default_hooks_path("crate::ExampleHooks")
        .compile(&["proto/echo.proto"], &["proto"])
        .expect("compile echo");
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("noop");
    std::fs::create_dir_all(&out).unwrap();
    tonic_build::configure()
        .out_dir(out)
        .compile(&["proto/echo.proto"], &["proto"])
        .expect("compile default hooks");
}
