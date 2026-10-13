"""Small Python-to-Go bridge to the official VibeSys evaluator SDK.

The SDK owns schema validation and protocol serialization. Python owns workload
execution and metric calculation, and exchanges ordinary JSON with this helper.
"""

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import subprocess

OUTPUT_FLAG = "--vs-output"


@dataclass(frozen=True)
class MetricSpec:
    unit: str
    direction: str
    required: bool = True


class ProtocolReport:
    def __init__(self, output: Path | None):
        self.output = output
        self.process = None
        self.outcome = False

    def __enter__(self):
        self.process = subprocess.Popen(
            [
                "go",
                "run",
                "-mod=readonly",
                ".",
                str(self.output) if self.output else "",
            ],
            cwd=Path(__file__).parent / "reporter",
            stdin=subprocess.PIPE,
            text=True,
        )
        return self

    def _send(self, message):
        self.process.stdin.write(json.dumps(message, allow_nan=False) + "\n")
        self.process.stdin.flush()

    def declare(self, specs):
        self._send({"schema": {key: asdict(value) for key, value in specs.items()}})

    def emit(self, values):
        self._send({"values": values})
        self.outcome = True

    def fail(self, message):
        self._send({"error": message})
        self.outcome = True

    def __exit__(self, kind, error, traceback):
        if not self.outcome:
            self.fail(str(error) if error else "evaluation ended without a result")
        self.process.stdin.close()
        code = self.process.wait(timeout=120)
        if code and error is None:
            raise RuntimeError(f"VibeSys SDK reporter exited {code}")
