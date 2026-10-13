import pytest
from exp_runner.runner.process_runner import SERVICES, local_configs


def test_local_deployment_preserves_workload_and_input():
    hotel = {s: {"ip": "dns", "port": 8660, "replicas": 1} for s in SERVICES}
    generator = {"Addr": "http://dns:8660", "Rps": [600], "DurationSecs": 20}
    actual, load = local_configs(hotel, generator)
    assert hotel["frontend"]["ip"] == "dns"
    assert generator["Addr"] == "http://dns:8660"
    assert load == {**generator, "Addr": "http://127.0.0.1:8660"}
    assert actual["reservation"]["mongodbAddr"] == "mongodb://127.0.0.1:27005"
    assert actual["reservation"]["redisAddr"] == "redis://127.0.0.1:11005"


def test_process_backend_rejects_multiple_replicas():
    hotel = {s: {"replicas": 2} for s in SERVICES}
    with pytest.raises(ValueError, match="exactly one"):
        local_configs(hotel, {})
