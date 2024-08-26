from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent


@pytest.fixture(autouse=True)
def change_test_dir(request, monkeypatch):
    """Credit: https://stackoverflow.com/a/62055409"""
    monkeypatch.chdir(request.fspath.dirname)
