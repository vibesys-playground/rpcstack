import socket

import pytest
from exp_runner.runner.process_runner import (
    SERVICES,
    check_ports_available,
    local_configs,
)


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


def test_port_check_rejects_active_listener():
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        with pytest.raises(OSError):
            check_ports_available([listener.getsockname()[1]])


def test_port_check_allows_previous_server_time_wait():
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        listener.listen(1)
        with socket.create_connection(("127.0.0.1", port)) as client:
            server, _ = listener.accept()
            # Make the server the active closer, leaving its local port in TIME_WAIT.
            server.close()
            assert client.recv(1) == b""
    with socket.socket() as old_probe:
        with pytest.raises(OSError):
            old_probe.bind(("127.0.0.1", port))
    check_ports_available([port])
