"""
Hotel application plugin.
"""

import copy
import json
import logging
import re
import shlex
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Tuple

import yaml

if TYPE_CHECKING:
    from ..config import ExperimentConfig

from ..deployment_manager import TaskSpec
from ..executor import CommandExecutor, MockCommandExecutor, SubprocessExecutor
from ..naming import generate_project_name
from .base import AppBuilder, AppPlugin, DockerConfig
from .utils import (
    get_docker_progress_flag,
    normalize_features_to_tag,
)

logger = logging.getLogger(__name__)


def create_gen_config_dict(
    *,
    template_config: dict,
    frontend_host: str = "hotel_frontend",
) -> dict:
    """
    Generate project-specific gen_config.json content with namespaced frontend address.

    Updates the "Addr" field to point to `frontend_host` (a compose alias for
    Docker, or a k8s Service hostname like `{project}-hotel-frontend-service-1`
    for Kubernetes).
    """
    config = copy.deepcopy(template_config)

    # Parse the original address
    protocol = "http"
    port = "8660"
    if "Addr" in config:
        addr = config["Addr"]
        if "://" in addr:
            protocol, rest = addr.split("://", 1)
            if ":" in rest:
                _, port = rest.rsplit(":", 1)

    config["Addr"] = f"{protocol}://{frontend_host}:{port}"

    return config


def create_hotel_config_dict(
    *,
    template_config: dict,
    project_name: str,
) -> dict:
    """
    Generate project-specific hotel.json with namespaced service names.

    Transforms service IPs from static names to project-prefixed container names:
    - "local-rate-service" -> "{project_name}-rate-service"
    - "rate_mongo" -> "{project_name}-rate-mongo-1"
    """
    config = copy.deepcopy(template_config)

    # The runner consumes this key, but the strict Rust config parser does not.
    config.pop("cpus_per_replica", None)

    # Service name mappings: config key -> (compose service name, is_scaled)
    # Scaled services use service name for DNS load balancing
    # Non-scaled services get explicit -1 suffix
    service_mappings = {
        "geo": ("geo-service", True),
        "profile": ("profile-service", True),
        "rate": ("rate-service", True),
        "recommendation": ("recommendation-service", True),
        "reservation": ("reservation-service", True),
        "review": ("review-service", True),
        "search": ("search-service", True),
        "user": ("user-service", True),
        # Frontend is not scaled, but we reference it by service name
        "frontend": ("hotel-frontend-service", False),
    }

    # Update service IPs
    for key, (service_name, is_scaled) in service_mappings.items():
        if key in config:
            if is_scaled:
                # Scaled services: use service name (docker-compose load balances)
                config[key]["ip"] = f"{project_name}-{service_name}"
            else:
                # Non-scaled services: use container name with -1 suffix
                config[key]["ip"] = f"{project_name}-{service_name}-1"

    # Update infrastructure addresses (mongo, redis)
    # These are always single containers with -1 suffix
    infra_mappings = {
        "profile": {"mongodbAddr": "profile-mongo", "redisAddr": "profile-redis"},
        "rate": {"mongodbAddr": "rate-mongo", "redisAddr": "rate-redis"},
        "reservation": {
            "mongodbAddr": "reservation-mongo",
            "redisAddr": "reservation-redis",
        },
        "user": {"mongodbAddr": "user-mongo"},
    }

    for service, addrs in infra_mappings.items():
        if service in config:
            for addr_key, infra_service in addrs.items():
                if addr_key in config[service]:
                    # Parse and update the address
                    old_addr = config[service][addr_key]
                    if "://" in old_addr:
                        protocol, rest = old_addr.split("://", 1)
                        hostname, *port_parts = rest.split(":", 1)
                        new_hostname = f"{project_name}-{infra_service}-1"
                        if port_parts:
                            new_addr = f"{protocol}://{new_hostname}:{port_parts[0]}"
                        else:
                            new_addr = f"{protocol}://{new_hostname}"
                        config[service][addr_key] = new_addr
    return config


class HotelBuilder(AppBuilder):
    """Compile the isolated hotel workspace on the host; package each binary.

    The surrounding experiment owns serialization of the shared image tags.
    Framework dependencies are resolved by the workspace's local Cargo patches.
    """

    def build(
        self,
        *,
        repo_root,
        app_dir,
        features=None,
        rust_log="info",
        no_cache=False,
        gen_config_path=None,
        dry_run=False,
        build_logs_dir=None,
        executor=None,
    ):
        import os
        import shutil

        executor = executor or SubprocessExecutor()
        if gen_config_path is None:
            raise ValueError("gen_config_path is required")
        tag = normalize_features_to_tag(features)
        binaries = [
            "hotel_client_bench",
            "hotel_frontend",
            "hotel_geo",
            "hotel_rate",
            "hotel_review",
            "hotel_search",
            "hotel_profile",
            "hotel_reservation",
            "hotel_user",
            "hotel_recommendation",
        ]
        target = Path(
            os.environ.get("CARGO_TARGET_DIR", str(repo_root / "target"))
        ).resolve()
        build = ["cargo", "build", "--locked", "--release", "-p", "hotel", "--bins"]
        if features:
            build += ["--features", features]
        commands = [build]
        for name in binaries:
            commands.append(
                [
                    "docker",
                    "build",
                    "-f",
                    "exp_runner/common/docker-build/Dockerfile",
                    "--build-arg",
                    f"BINARY_NAME={name}",
                    "--build-arg",
                    f"LOG_LEVEL={rust_log}",
                    "--build-arg",
                    f"GEN_CONFIG_PATH={gen_config_path.relative_to(repo_root)}",
                    "-t",
                    f"{name}:{tag}",
                    ".",
                ]
            )
        if dry_run:
            return commands
        executor.run(build, cwd=repo_root, check=True)
        bundle = repo_root / ".build-bin"
        bundle.mkdir(exist_ok=True)
        for name in binaries:
            shutil.copy2(target / "release" / name, bundle / name)
        for command in commands[1:]:
            executor.run(command, cwd=repo_root, check=True)
        return None


class HotelApp(AppPlugin):
    """
    Plugin for the hotel reservation microservices application.

    The hotel app consists of multiple microservices (rate, profile, reservation,
    geo, search, user, recommendation, review) that can be replicated.
    """

    def get_app_name(self) -> str:
        return "hotel"

    @property
    def supports_k8s(self) -> bool:
        return True

    def load_app_config(self, config_path: Path) -> dict:
        """Load hotel.json configuration file."""
        with open(config_path) as f:
            return json.load(f)

    def generate_env_vars(
        self, gen_config: dict, app_config: Optional[dict], app_dir: Path
    ) -> dict:
        """
        Generate environment variables for hotel application.

        Extracts frontend port from gen_config and replica counts from hotel.json.
        """
        env_vars = {}

        # Extract frontend port from address (e.g., "http://[::1]:8659" -> "8659")
        addr = gen_config.get("Addr", "")
        match = re.search(r":(\d+)$", addr)
        if match:
            env_vars["FRONTEND_PORT"] = match.group(1)
        else:
            raise ValueError(f"Unable to extract port from address: {addr}")

        # Set replica counts from hotel.json
        default_replicas = 1
        services = [
            "rate",
            "profile",
            "reservation",
            "geo",
            "search",
            "user",
            "recommendation",
            "review",
        ]

        if app_config:
            for service in services:
                env_key = f"{service.upper()}_REPLICAS"
                replicas = app_config.get(service, {}).get("replicas", default_replicas)
                env_vars[env_key] = str(replicas)
            frontend_replicas = app_config.get("frontend", {}).get(
                "replicas", default_replicas
            )
            env_vars["FRONTEND_REPLICAS"] = str(frontend_replicas)
        else:
            # Use defaults if no config provided
            for service in services:
                env_key = f"{service.upper()}_REPLICAS"
                env_vars[env_key] = str(default_replicas)
            env_vars["FRONTEND_REPLICAS"] = str(default_replicas)

        # A single experiment knob controls both the container CPU quota and
        # the Tokio worker count.
        cpus_per_replica = (app_config or {}).get("cpus_per_replica", 4)
        env_vars["CPUS_PER_REPLICA"] = str(cpus_per_replica)

        # Set default log level if not specified
        if "LOG_LEVEL" not in env_vars:
            env_vars["LOG_LEVEL"] = "info"

        return env_vars

    def get_docker_config(self) -> DockerConfig:
        """Return Docker configuration for hotel application."""
        return DockerConfig(
            compose_file="scripts/local/containers+svcs.yaml",
            network_name="local_hotel_network",
            loadgen_image_name="hotel_client_bench:<features>",  # Actual tag is dynamic based on features
            loadgen_binary_name="hotel_client_bench",
            app_config_filename="hotel.json",
        )

    def get_container_names(self, env_vars: dict) -> list[str]:
        """
        Get list of container names for hotel application.

        Includes frontend and replicated service containers based on
        replica counts from environment variables.
        """
        frontend_replicas = int(env_vars.get("FRONTEND_REPLICAS", 1))
        container_names = [
            f"local-hotel-frontend-service-{i}" for i in range(1, frontend_replicas + 1)
        ]

        # Services that can be replicated
        replicated_services = {
            "rate": int(env_vars.get("RATE_REPLICAS", 1)),
            "profile": int(env_vars.get("PROFILE_REPLICAS", 1)),
            "reservation": int(env_vars.get("RESERVATION_REPLICAS", 1)),
            "geo": int(env_vars.get("GEO_REPLICAS", 1)),
            "search": int(env_vars.get("SEARCH_REPLICAS", 1)),
            "user": int(env_vars.get("USER_REPLICAS", 1)),
        }

        # Generate container names for each replica
        for service, count in replicated_services.items():
            for i in range(1, count + 1):
                container_names.append(f"local-{service}-service-{i}")

        return container_names

    def create_builder(self) -> AppBuilder:
        """Create a builder instance for hotel application."""
        return HotelBuilder()

    def get_image_tag(self, features: Optional[str] = None) -> str:
        """
        Get the docker image tag for the given features.

        Args:
            features: Optional cargo features

        Returns:
            Docker image tag string
        """
        return normalize_features_to_tag(features)

    def prepare_workload(
        self,
        config: "ExperimentConfig",
        policy: str,
        iteration: int,
        output_dir: Path,
        repo_root: Path,
        use_k8s: bool = False,
        executor: Optional[CommandExecutor] = None,
    ) -> dict:
        """
        Prepare workload configuration and environment variables.
        """
        # Generate project name for namespace isolation
        project_name = generate_project_name(
            prefix="hotel",
            experiment_name=config.experiment_name,
            iteration=iteration,
            policy=policy,
        )

        # 1. Calculate configuration (Pure Logic)
        hotel_config_dict = create_hotel_config_dict(
            template_config=config.app_config,
            project_name=project_name,
        )

        # The frontend hostname differs between Docker (network alias
        # `hotel_frontend`) and k8s (Service `{project}-hotel-frontend-service-1`
        # — see charts/hotel/templates/services.yaml, which appends `-1` to
        # non-scaled services).
        if use_k8s:
            frontend_host = f"{project_name}-hotel-frontend-service-1"
        else:
            frontend_host = "hotel_frontend"

        gen_config_dict = create_gen_config_dict(
            template_config=config.gen_config,
            frontend_host=frontend_host,
        )

        # 2. Generate config files (Effects)
        output_dir.mkdir(parents=True, exist_ok=True)

        project_config_path = output_dir / "hotel.json"
        with project_config_path.open("w") as f:
            json.dump(hotel_config_dict, f, indent=2)

        project_gen_config_path = output_dir / "gen_config.json"
        with project_gen_config_path.open("w") as f:
            json.dump(gen_config_dict, f, indent=2)

        # 3. Generate environment variables
        env_vars = self.generate_env_vars(
            config.gen_config, config.app_config, config.app_dir
        )

        # Add image tag based on policy/features
        tag = self.get_image_tag(features=policy)
        # Write policy_param.json via the shared AppPlugin helper.
        project_policy_params_path = self._write_policy_params(
            output_dir, config.policy_params, policy
        )

        env_vars["HOTEL_IMAGE_TAG"] = tag if tag else "latest"
        env_vars["DOCKER_COMPOSE_PROJECT_NAME"] = project_name
        env_vars["PROJECT_CONFIG_PATH"] = str(project_config_path.resolve())
        env_vars["POLICY_PARAMS_PATH"] = str(project_policy_params_path)

        if use_k8s:
            self._prepare_k8s_workload(
                output_dir=output_dir,
                project_name=project_name,
                hotel_config=hotel_config_dict,
                policy_params_path=project_policy_params_path,
                image_tag=tag if tag else "latest",
                log_level=env_vars.get("LOG_LEVEL", "info"),
                env_vars=env_vars,
            )

        return env_vars

    def _prepare_k8s_workload(
        self,
        *,
        output_dir: Path,
        project_name: str,
        hotel_config: dict,
        policy_params_path: Path,
        image_tag: str,
        log_level: str,
        env_vars: dict,
    ) -> None:
        """
        Build a Helm `values.yaml` for the hotel chart and set HELM_VALUES_FILE.

        The service/infra lists mirror `apps/hotel/scripts/local/containers+svcs.yaml`;
        hostnames are kept in lock-step with `create_hotel_config_dict` so the
        addresses baked into hotel.json resolve via k8s DNS.
        """

        # Ports come from hotel.json. Fall back to the docker-compose defaults
        # when a field is absent (matches apps/hotel/scripts/local/containers+svcs.yaml).
        def svc_port(name: str, default: int) -> int:
            return int(hotel_config.get(name, {}).get("port", default))

        def svc_replicas(name: str) -> int:
            return int(hotel_config.get(name, {}).get("replicas", 1))

        services = [
            # Scaled microservices: k8s Service DNS (no `-1`) load-balances
            # across pods, matching `{project}-{svc}` scaled addresses.
            {
                "name": "rate-service",
                "binary": "hotel_rate",
                "replicas": svc_replicas("rate"),
                "port": svc_port("rate", 8663),
                "scaled": True,
            },
            {
                "name": "profile-service",
                "binary": "hotel_profile",
                "replicas": svc_replicas("profile"),
                "port": svc_port("profile", 8662),
                "scaled": True,
            },
            {
                "name": "reservation-service",
                "binary": "hotel_reservation",
                "replicas": svc_replicas("reservation"),
                "port": svc_port("reservation", 8666),
                "scaled": True,
            },
            {
                "name": "geo-service",
                "binary": "hotel_geo",
                "replicas": svc_replicas("geo"),
                "port": svc_port("geo", 8661),
                "scaled": True,
            },
            {
                "name": "search-service",
                "binary": "hotel_search",
                "replicas": svc_replicas("search"),
                "port": svc_port("search", 8668),
                "scaled": True,
            },
            {
                "name": "user-service",
                "binary": "hotel_user",
                "replicas": svc_replicas("user"),
                "port": svc_port("user", 8669),
                "scaled": True,
            },
            # Frontend is non-scaled — exp_runner writes `{project}-hotel-frontend-service-1`
            # into gen_config.json, so the Service must carry the literal `-1`.
            {
                "name": "hotel-frontend-service",
                "binary": "hotel_frontend",
                "replicas": svc_replicas("frontend"),
                "port": svc_port("frontend", 8660),
                "scaled": False,
            },
        ]

        # Mongo/redis ports match the docker-compose layout. Each infra entry
        # is a single-replica Deployment + a `{project}-{name}-1` Service.
        infra = [
            {
                "name": "rate-mongo",
                "image": "mongo:7.0",
                "command": ["--port", "27003"],
                "port": 27003,
                "volumeClaim": "rate",
            },
            {
                "name": "rate-redis",
                "image": "redis:7.2",
                "command": ["redis-server", "--port", "11003", "--io-threads", "4"],
                "port": 11003,
            },
            {
                "name": "profile-mongo",
                "image": "mongo:7.0",
                "command": ["--port", "27004"],
                "port": 27004,
                "volumeClaim": "profile",
            },
            {
                "name": "profile-redis",
                "image": "redis:7.2",
                "command": ["redis-server", "--port", "11004", "--io-threads", "4"],
                "port": 11004,
            },
            {
                "name": "reservation-mongo",
                "image": "mongo:7.0",
                "command": ["--port", "27005"],
                "port": 27005,
                "volumeClaim": "reservation",
            },
            {
                "name": "reservation-redis",
                "image": "redis:7.2",
                "command": ["redis-server", "--port", "11005", "--io-threads", "4"],
                "port": 11005,
            },
            {
                "name": "user-mongo",
                "image": "mongo:7.0",
                "command": ["--port", "27006"],
                "port": 27006,
                "volumeClaim": "user",
            },
        ]

        pvcs = [{"name": n} for n in ("rate", "profile", "reservation", "user")]

        with policy_params_path.open() as f:
            policy_params_text = f.read()

        values = {
            "fullnameOverride": project_name,
            "image": {
                "repository": "",
                "tag": image_tag,
                "pullPolicy": "Never",
            },
            "logLevel": log_level,
            "service": {"type": "ClusterIP"},
            "services": services,
            "infra": infra,
            "pvcs": pvcs,
            "configMaps": {
                "enabled": True,
                "hotelJson": json.dumps(hotel_config, indent=2),
                "policyParamsJson": policy_params_text,
            },
        }

        values_path = output_dir / "values.yaml"
        with values_path.open("w") as f:
            yaml.dump(values, f, sort_keys=False)

        env_vars["HELM_VALUES_FILE"] = str(values_path.resolve())

    def get_deployment_location(
        self, output_dir: Path, use_k8s: bool, repo_root: Path
    ) -> Tuple[Path, str]:
        if use_k8s:
            return repo_root / "charts" / "hotel", "."

        # Default app dir is config.app_dir which is passed to ExpDriver -> deployment.start
        # But here we return (deploy_root, deploy_file).
        # We need to return the directory containing the compose file.
        # DockerConfig says: compose_file="scripts/local/containers+svcs.yaml"
        # So it expects to be run from `apps/hotel`.
        return repo_root / "apps/hotel", "scripts/local/containers+svcs.yaml"

    def get_loadgen_spec(
        self,
        output_dir: Path,
        features: Optional[str],
        env_vars: dict,
        use_k8s: bool,
    ) -> TaskSpec:
        project_name = env_vars.get("DOCKER_COMPOSE_PROJECT_NAME")

        # Image
        tag = self.get_image_tag(features)
        image = f"hotel_client_bench:{tag}" if tag else "hotel_client_bench:latest"
        binary = "hotel_client_bench"

        # Network — unused in k8s mode (pods attach to the cluster network).
        if use_k8s:
            network = None
        else:
            network = (
                f"{project_name}_hotel_network"
                if project_name
                else "local_hotel_network"
            )

        # Env Vars
        task_env = {
            "BINARY_NAME": binary,
            "LOG_LEVEL": env_vars.get("LOG_LEVEL", "info"),
        }
        task_env.update(env_vars)

        # Config mounting
        # Hotel needs gen_config.json mounted as well?
        # HotelLoadGenerator didn't mount gen_config.json explicitly in the old code?
        # Let's check `HotelLoadGenerator.run` -> `LoadGenerator.run` which DOES mount it
        # if `gen_config_path` is passed.
        # And `HotelApp.run_workload` passed `project_gen_config_path`.

        gen_config_path = output_dir / "gen_config.json"
        volumes = {}
        if gen_config_path.exists():
            volumes[str(gen_config_path)] = "/usr/gen_config.json"
        volumes.update(self._policy_params_loadgen_mount(output_dir))

        # In k8s, the pod must stay alive after the load generator exits so
        # that ExpDriver can `kubectl cp` artifacts out of the container —
        # mirroring what tracebench does. The entrypoint script always runs the
        # loadgen binary directly, so we override `command` to wrap it.
        command = None
        if use_k8s:
            command = [
                "/bin/sh",
                "-c",
                "/usr/entrypoint.sh || exit $?; "
                "while true; do echo HOTEL_LOADGEN_DONE; sleep 10; done",
            ]

        return TaskSpec(
            name=f"{project_name}-loadgen" if project_name else "hotel-loadgen",
            image=image,
            env_vars=task_env,
            network=network,
            volumes=volumes,
            cleanup=True,
            artifacts=[("/tmp/rpcstack-load-gen/.", ".")],
            command=command,
            wait_for_log_pattern="HOTEL_LOADGEN_DONE" if use_k8s else None,
        )

    def get_required_images(self, features: Optional[str] = None) -> list[str]:
        tag = self.get_image_tag(features) or "latest"
        binaries = [
            "hotel_client_bench",
            "hotel_frontend",
            "hotel_geo",
            "hotel_rate",
            "hotel_review",
            "hotel_search",
            "hotel_profile",
            "hotel_reservation",
            "hotel_user",
            "hotel_recommendation",
        ]
        images = [f"{b}:{tag}" for b in binaries]
        # Stateful infra images — loaded into kind so tests work offline.
        images.extend(["mongo:7.0", "redis:7.2"])
        return images
