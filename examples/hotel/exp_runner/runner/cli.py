"""Hotel-only entry point for the extracted experiment runner."""

import argparse
import logging
import signal
from pathlib import Path
from .apps import get_app_plugin
from .config import ExperimentConfig
from .experiment import Experiment


def main():
    def terminate(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, terminate)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["run"])
    p.add_argument("app", choices=["hotel"])
    p.add_argument("experiment")
    p.add_argument("--rm-data", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--deployment", choices=["docker", "process"], default="docker")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    root = Path(__file__).resolve().parents[2]
    app = get_app_plugin(args.app)
    config = ExperimentConfig.load(args.experiment, args.app, root, app)
    if args.deployment == "process":
        from .process_runner import run_process

        run_process(config, root, dry_run=args.dry_run, overwrite=args.rm_data)
    else:
        Experiment(app, config, root, rm_data=args.rm_data, dry_run=args.dry_run).run()
