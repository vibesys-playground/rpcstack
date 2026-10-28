fn main() {
    tonic_build::configure()
        .compile(&["proto/credit.proto"], &["proto"])
        .unwrap();
}
