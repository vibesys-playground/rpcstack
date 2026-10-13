#!/usr/bin/env python3
"""Reject policy coupling and external path dependencies in the standalone tree."""
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--locked", "--format-version", "1"], cwd=root))
allowed = {
    "rpcstack", "rpcstack-wire", "rpcstack-tonic", "rpcstack-sched", "rpcstack-call",
    "tonic", "tonic-build", "hyper", "tokio", "multiqueue-prio-queue",
    "rpcstack-echo", "rajomon", "tailclipper", "paper-rpc",
    "masa", "masa-core", "masa-policy", "masa-semantics",
    "rpcstack-dagor", "rpcstack-breakwater", "rpcstack-hedged", "rpcstack-c3",
    "rpcstack-rifl", "rpcstack-r2p2", "rpcstack-persephone", "rpcstack-protego", "rpcstack-aequitas", "rpcstack-testkit", "rpcstack-policy-composition",
}
for package in metadata["packages"]:
    if package["name"].startswith("masa"):
        assert Path(package["manifest_path"]).is_relative_to(root / "examples" / "masa"), package["name"]
    if package["source"] is not None:
        continue
    assert package["name"] in allowed, package["name"]
    assert Path(package["manifest_path"]).is_relative_to(root), package["manifest_path"]
    deps = {dep["name"] for dep in package["dependencies"]}
    if Path(package["manifest_path"]).is_relative_to(root / "libs"):
        assert not deps & {"rajomon", "tailclipper"}, deps
        for dep in package["dependencies"]:
            if dep.get("path"):
                assert not Path(dep["path"]).is_relative_to(root / "examples"), dep
    if package["name"] == "rpcstack-sched":
        assert not deps, deps
    if package["name"] == "rpcstack-wire":
        assert deps <= {"serde", "base64", "bincode"}, deps
    if package["name"] == "rpcstack":
        assert deps <= {"rpcstack-wire", "tonic", "http", "serde", "smallvec"}, deps
    if package["name"] == "rpcstack-tonic":
        assert deps <= {"rpcstack", "tonic", "http", "serde"}, deps
    if package["name"] in {"tokio", "hyper", "tonic"}:
        assert not deps & {"rpcstack", "rpcstack-tonic"}, deps
print("Standalone dependency boundaries passed")
