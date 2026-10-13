fn main() {
    tonic_build::configure()
        .compile(&["proto/probe.proto"], &["proto"])
        .unwrap();
}
