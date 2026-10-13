"""Validate persistence and observation-independent boot in the shipped stack.

No Docker daemon, service boot, credentials or environment interpolation.
The paths are the actual runtime paths established by live metadata.
"""
from pathlib import Path, PurePosixPath

import yaml


COMPOSE = Path(__file__).resolve().parents[2] / "deploy/compose/docker-compose.yml"


def stack():
    return yaml.safe_load(COMPOSE.read_text())


def test_kafka_actual_log_directories_survive_container_recreation():
    compose = stack()
    kafka = compose["services"]["kafka"]
    targets = []
    for mount in kafka.get("volumes", []):
        if isinstance(mount, str):
            source, target, *_ = mount.split(":")
            if source in compose.get("volumes", {}):
                targets.append(PurePosixPath(target))
    for path in kafka["environment"]["KAFKA_LOG_DIRS"].split(","):
        assert any(PurePosixPath(path) == target or target in PurePosixPath(path).parents
                   for target in targets), f"Kafka actual logs are not on a named volume: {path}"


def test_core_gateway_boot_does_not_wait_for_observation_archive():
    dependencies = stack()["services"]["ocpp"]["depends_on"]
    assert "clickhouse" not in dependencies
    assert "migrate-clickhouse" not in dependencies
    assert dependencies["migrate-postgres"]["condition"] == "service_completed_successfully"


def test_new_kafka_volume_is_owned_before_unprivileged_broker_starts():
    services = stack()["services"]
    dependency = services["kafka"].get("depends_on", {}).get("kafka-storage-init")
    assert dependency == {"condition": "service_completed_successfully"}
    initializer = services["kafka-storage-init"]
    assert initializer["image"] == services["kafka"]["image"]
    assert initializer["user"] == "0:0"
    assert initializer["volumes"] == ["kafka_data:/tmp/kraft-combined-logs"]
    # Only the mounted directory changes ownership; old copied file ownership
    # must be preserved and checked in the explicit migration preflight.
    assert initializer["entrypoint"] == ["chown", "1000:1000", "/tmp/kraft-combined-logs"]
    assert initializer.get("restart", "no") == "no"


def test_long_running_services_recover_after_process_or_host_restart():
    services = stack()["services"]
    for name in ("postgres", "redis", "kafka", "clickhouse", "ocpp", "clickhouse-ingestor"):
        assert services[name].get("restart") == "unless-stopped", name
    # Migration jobs retain explicit finite failure handling.
    for name in ("migrate-postgres", "migrate-clickhouse"):
        assert services[name].get("restart", "no") != "unless-stopped", name
