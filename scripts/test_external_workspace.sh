#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"
probe_dir=$(mktemp -d)
trap 'rm -rf "$probe_dir"' EXIT
python3 - "$PWD" "$probe_dir" <<'PY'
import json
from pathlib import Path
import sys
root, probe = map(Path, sys.argv[1:])
def path(relative):
    return json.dumps(str(root / relative))
(probe / 'Cargo.toml').write_text(f'''[package]
name = "rpcstack-external-probe"
version = "0.0.0"
edition = "2021"
[workspace]
[dependencies]
rpcstack-call = {{ path = {path("libs/rpcstack-call")} }}
rpcstack-tonic = {{ path = {path("libs/rpcstack-tonic")} }}
tonic = {{ path = {path("libs/tonic/tonic")} }}
tokio = {{ path = {path("libs/tokio/tokio")}, features = ["macros", "rt"] }}
[patch.crates-io]
tokio = {{ path = {path("libs/tokio/tokio")} }}
hyper = {{ path = {path("libs/hyper")} }}
''')
(probe / 'src').mkdir()
(probe / 'src/main.rs').write_text('''#[tokio::main(flavor = "current_thread")]
async fn main() {
    let _channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let _context = rpcstack_tonic::ServerContext::<()>::try_new("external").unwrap();
    let (answer, _) = rpcstack_call::Stage::new(1).run(async { 42 }).await;
    assert_eq!(answer, 42);
}
''')
PY
# Deliberately do not reuse this repository's lockfile: that hid a second Tokio.
cargo run --quiet --manifest-path "$probe_dir/Cargo.toml"
cargo metadata --locked --format-version 1 --manifest-path "$probe_dir/Cargo.toml" > "$probe_dir/metadata.json"
python3 - "$probe_dir/metadata.json" "$PWD" <<'PY'
import json
from pathlib import Path
import sys
packages = json.loads(Path(sys.argv[1]).read_text())["packages"]
tokio = [p for p in packages if p["name"] == "tokio"]
assert len(tokio) == 1, [(p["version"], p["source"]) for p in tokio]
assert Path(tokio[0]["manifest_path"]) == Path(sys.argv[2]) / "libs/tokio/tokio/Cargo.toml"
print("External workspace resolves and runs with exactly the vendored Tokio")
PY
