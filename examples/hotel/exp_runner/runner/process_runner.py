"""Run the hotel experiment as ordinary processes inside one sandbox.

The application and load generator are identical to the Compose deployment.
This backend deliberately supports one replica per service, loopback RPC, and
CPU affinity rather than container CPU quotas. Compare policies within a
backend: its absolute measurements are not interchangeable with Compose.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time

from .config import ExperimentConfig

SERVICES = ("rate", "profile", "reservation", "user", "geo", "search", "frontend")
DATABASES = {"rate": 27003, "profile": 27004, "reservation": 27005, "user": 27006}


def local_configs(hotel: dict, generator: dict) -> tuple[dict, dict]:
    """Change only deployment addresses; keep workload semantics unchanged."""
    hotel, generator = copy.deepcopy(hotel), copy.deepcopy(generator)
    for service in SERVICES:
        cfg = hotel[service]
        if cfg.get("replicas", 1) != 1:
            raise ValueError("process deployment supports exactly one replica")
        cfg["ip"] = "127.0.0.1"
        if service in DATABASES:
            cfg["mongodbAddr"] = f"mongodb://127.0.0.1:{DATABASES[service]}"
            if service != "user":
                cfg["redisAddr"] = f"redis://127.0.0.1:{DATABASES[service] - 16000}"
    generator["Addr"] = f"http://127.0.0.1:{hotel['frontend']['port']}"
    return hotel, generator


def check_ports_available(ports: list[int]) -> None:
    """Reject active listeners while allowing a previous run's TIME_WAIT sockets."""
    for port in ports:
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
            probe.listen(1)


class Processes:
    """Own only the child process groups created by this experiment."""

    def __init__(self, logs: Path, cpus: list[int]):
        self.logs, self.cpus = logs, cpus
        self.children: list[tuple[str, subprocess.Popen]] = []
        self.logs.mkdir(parents=True, exist_ok=True)

    def start(self, name: str, argv: list[str], *, env: dict, cwd: Path):
        with (self.logs / f"{name}.log").open("w") as log:
            child = subprocess.Popen(
                ["taskset", "-c", ",".join(map(str, self.cpus)), *argv],
                cwd=cwd,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        self.children.append((name, child))
        return child

    def healthy(self):
        failed = [
            (name, child.returncode)
            for name, child in self.children
            if child.poll() is not None
        ]
        if failed:
            raise RuntimeError(f"processes exited: {failed}; see {self.logs}")

    def ready(self, ports: list[int]):
        deadline = time.monotonic() + 120
        remaining = set(ports)
        while remaining:
            self.healthy()
            for port in tuple(remaining):
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        remaining.remove(port)
                except OSError:
                    pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"services did not listen: {remaining}")
            time.sleep(0.1)

    def close(self):
        for _, child in reversed(self.children):
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
        for _, child in reversed(self.children):
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()


def run_process(
    config: ExperimentConfig, root: Path, *, dry_run=False, overwrite=False
):
    hotel, generator = local_configs(config.app_config, config.gen_config)
    build = ["cargo", "build", "--locked", "--release", "-p", "hotel", "--bins"]
    if config.policies != ["rpcstack"]:
        raise ValueError("process backend uses the rpcstack policy slot")
    if dry_run:
        print(build, hotel, generator)
        return
    if config.out_dir.exists() and any(config.out_dir.iterdir()):
        if not overwrite:
            raise FileExistsError(
                f"{config.out_dir} already has results; use --rm-data explicitly"
            )
        shutil.rmtree(config.out_dir)
    for tool in ("mongod", "redis-server", "taskset"):
        if not shutil.which(tool):
            raise RuntimeError(f"process deployment requires {tool} on PATH")
    subprocess.run(build, cwd=root, check=True)
    target = (
        Path(os.environ.get("CARGO_TARGET_DIR", root / "target")).resolve() / "release"
    )
    cpus = sorted(os.sched_getaffinity(0))
    # Eight CPUs for the services and their stores, four for the load generator.
    # Both baseline and candidate use the same immutable split.
    if len(cpus) < 12:
        raise RuntimeError(
            "process benchmark requires at least twelve available logical CPUs"
        )
    env = dict(os.environ, RUST_LOG="info", CPUS_PER_REPLICA="4")
    ports = (
        [hotel[s]["port"] for s in SERVICES]
        + list(DATABASES.values())
        + [11003, 11004, 11005]
    )
    # Refuse to run into another deployment rather than terminating it.
    check_ports_available(ports)
    for iteration in range(config.get_repeats()):
        out = config.out_dir / str(iteration) / "rpcstack"
        out.mkdir(parents=True, exist_ok=True)
        (out / "hotel.json").write_text(json.dumps(hotel, indent=2))
        (out / "gen_config.json").write_text(json.dumps(generator, indent=2))
        (out / "deployment.json").write_text(
            json.dumps(
                {
                    "backend": "process",
                    "service_cpus": cpus[:8],
                    "loadgen_cpus": cpus[8:12],
                },
                indent=2,
            )
        )
        with tempfile.TemporaryDirectory(prefix="rpcstack-hotel-") as temp:
            processes = Processes(out / "logs", cpus[:8])
            try:
                for name, port in DATABASES.items():
                    db = Path(temp) / name
                    db.mkdir()
                    processes.start(
                        name + "-mongo",
                        [
                            "mongod",
                            "--port",
                            str(port),
                            "--bind_ip",
                            "127.0.0.1",
                            "--dbpath",
                            str(db),
                            "--wiredTigerCacheSizeGB",
                            ".25",
                        ],
                        env=env,
                        cwd=db,
                    )
                    if name != "user":
                        processes.start(
                            name + "-redis",
                            [
                                "redis-server",
                                "--port",
                                str(port - 16000),
                                "--bind",
                                "127.0.0.1",
                                "--save",
                                "",
                                "--appendonly",
                                "no",
                                "--io-threads",
                                "4",
                            ],
                            env=env,
                            cwd=db,
                        )
                processes.ready(list(DATABASES.values()) + [11003, 11004, 11005])
                for name in SERVICES:
                    processes.start(
                        name,
                        [
                            str(target / f"hotel_{name}"),
                            "--config",
                            str(out / "hotel.json"),
                        ],
                        env=env,
                        cwd=out,
                    )
                processes.ready([hotel[s]["port"] for s in SERVICES])
                with (out / "semantic-check.log").open("w") as log:
                    subprocess.run(
                        [
                            str(target / "hotel_probe"),
                            generator["Addr"],
                            hotel["reservation"]["mongodbAddr"],
                        ],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=True,
                        timeout=120,
                    )
                with (out / "loadgen.log").open("w") as log:
                    subprocess.run(
                        [
                            "taskset",
                            "-c",
                            ",".join(map(str, cpus[8:12])),
                            str(target / "hotel_client_bench"),
                            "--gen-config",
                            str(out / "gen_config.json"),
                            "--output-path",
                            str(out),
                        ],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=True,
                        timeout=1800,
                    )
                processes.healthy()
            finally:
                processes.close()
    (config.out_dir / "DONE").touch()
