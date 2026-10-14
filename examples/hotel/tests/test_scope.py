"""Exercise the real scope gate against a Git index and working tree."""

from pathlib import Path
import shutil
import subprocess
import sys

import pytest

TASK = Path(__file__).resolve().parents[3] / ".vibesys/tasks/hotel-goodput/benchmark"


@pytest.mark.parametrize(
    ("change", "accepted"),
    [
        ("added", False),
        ("removed_candidate", True),
        ("removed_protected", False),
        ("broken_symlink", False),
    ],
)
def test_scope_checks_present_additions_and_all_protected_files(
    tmp_path, change, accepted
):
    task = tmp_path / ".vibesys/tasks/hotel-goodput/benchmark"
    task.mkdir(parents=True)
    for name in ("rpcstack_hotel.py", "sdk_report.py"):
        shutil.copyfile(TASK / name, task / name)

    def git(*args):
        return subprocess.check_output(["git", *args], cwd=tmp_path, text=True).strip()

    git("init", "-q")
    protected = tmp_path / "protected.rs"
    protected.write_text("protected source\n")
    git("add", "protected.rs")
    digest = git("hash-object", "protected.rs")
    (task / "protected_files.txt").write_text(f"{digest} protected.rs\n")
    extra = tmp_path / "validation_recipes.json"
    if change == "removed_protected":
        protected.unlink()
    elif change == "broken_symlink":
        extra.symlink_to("missing-target")
    else:
        extra.write_text("{}\n")
        if change == "removed_candidate":
            git("add", extra.name)
            extra.unlink()

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import runpy, sys; from pathlib import Path; "
            "sys.path.insert(0, sys.argv[1]); "
            "runpy.run_path(sys.argv[1] + '/rpcstack_hotel.py')['check_scope'](Path.cwd())",
            str(task),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert (result.returncode == 0) == accepted, result.stderr
    if not accepted:
        assert "changes outside the agent-owned slots" in result.stderr
