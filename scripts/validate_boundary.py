#!/usr/bin/env python3
"""Reject policy coupling and external path dependencies in the standalone tree."""
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--locked", "--format-version", "1"], cwd=root))
allowed = {"rpcstack", "rpcstack-wire", "rpcstack-tonic", "rpcstack-sched",
           "tonic", "tonic-build", "hyper", "tokio", "multiqueue-prio-queue", "rpcstack-echo", "rpcstack-call"}
for package in metadata["packages"]:
    assert not package["name"].startswith("masa"), package["name"]
    if package["source"] is not None:
        continue
    assert package["name"] in allowed, package["name"]
    assert Path(package["manifest_path"]).is_relative_to(root), package["manifest_path"]
    deps = {dep["name"] for dep in package["dependencies"]}
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
