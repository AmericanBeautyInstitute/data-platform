"""Tests for the data platform."""

import tomllib


def test_version() -> None:
    """Tests that version is set to expected value."""
    with open("pyproject.toml", "rb") as f:
        version = tomllib.load(f)["project"]["version"]
    assert version == "0.1.0"
