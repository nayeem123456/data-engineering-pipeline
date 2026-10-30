"""
Unit Tests for Dockerfile and Docker Compose Configuration.
Validates service topology, port bindings, healthchecks, volume persistence, and environment contracts.
"""

from __future__ import annotations

import yaml

from src.common.config import config


def test_docker_compose_valid_yaml_and_topology():
    """Verify docker-compose.yml is valid YAML and defines all required services."""
    compose_path = config.PROJECT_ROOT / "docker-compose.yml"
    assert compose_path.exists(), "docker-compose.yml not found!"

    with open(compose_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert "services" in data, "No services defined in docker-compose.yml"
    services = data["services"]

    expected_services = {
        "postgres",
        "airflow-init",
        "airflow-webserver",
        "airflow-scheduler",
        "lakehouse-worker",
        "dashboard",
    }
    for svc in expected_services:
        assert svc in services, f"Required service '{svc}' missing from docker-compose.yml"


def test_docker_compose_healthchecks_and_volumes():
    """Verify essential services have healthchecks and persistent volumes."""
    compose_path = config.PROJECT_ROOT / "docker-compose.yml"
    with open(compose_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    services = data["services"]

    # Postgres checks
    assert "healthcheck" in services["postgres"]
    assert "postgres_data" in data.get("volumes", {})

    # Airflow webserver healthcheck
    assert "healthcheck" in services["airflow-webserver"]

    # Airflow scheduler healthcheck
    assert "healthcheck" in services["airflow-scheduler"]

    # Dashboard port and healthcheck
    assert "8501:8501" in services["dashboard"].get("ports", [])
    assert "healthcheck" in services["dashboard"]

    # Worker healthcheck
    assert "healthcheck" in services["lakehouse-worker"]


def test_dockerfile_syntax_and_directives():
    """Verify Dockerfile base image, dependencies, environment, and healthcheck."""
    dockerfile_path = config.PROJECT_ROOT / "Dockerfile"
    assert dockerfile_path.exists(), "Dockerfile not found!"

    content = dockerfile_path.read_text(encoding="utf-8")

    assert "python:3.11" in content
    assert "WORKDIR /app" in content
    assert "requirements.txt" in content
    assert "HEALTHCHECK" in content
    assert "EXPOSE" in content
