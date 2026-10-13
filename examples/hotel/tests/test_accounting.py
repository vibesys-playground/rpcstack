import importlib.util
from pathlib import Path
import sys

TASK = Path(__file__).resolve().parents[3] / ".vibesys/tasks/hotel-goodput/benchmark"
sys.path.insert(0, str(TASK))
from rpcstack_hotel import EvaluationError, check_light_load, read_points
import pytest


def test_goodput_counts_errors_and_late_successes_as_offered(tmp_path):
    for api in ("Search", "Reservation"):
        (tmp_path / f"r2_{api}.csv").write_text(
            "latency,slo,error\n50,100,/None\n150,100,/ClientMiss\n1,100,/Rejected\n"
        )
    (point,) = read_points(
        tmp_path, {"Rps": [2], "Apis": ["Search", "Reservation"], "DurationSecs": 3}
    )
    assert (point.offered, point.good, point.errors) == (6, 2, 2)
    assert point.goodput_rps == 2 / 3
    assert sorted(point.success_latencies_us) == [50, 50, 150, 150]
    with pytest.raises(EvaluationError):
        check_light_load(point)


def test_missing_api_and_under_delivered_workload_fail(tmp_path):
    config = {"Rps": [100], "Apis": ["Search"], "DurationSecs": 20}
    with pytest.raises(EvaluationError, match="no results"):
        read_points(tmp_path, config)
    (tmp_path / "r100_Search.csv").write_text("latency,slo,error\n1,100,/None\n")
    with pytest.raises(EvaluationError, match="under-delivered"):
        read_points(tmp_path, config)
