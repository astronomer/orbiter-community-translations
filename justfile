#!/usr/bin/env just --justfile
set dotenv-load := true
EXTRAS := "dev"
VERSION := `echo $(python -c 'from orbiter_translations.version import __version__; print(__version__)')`
PYTHON := `which python || which python3`

default:
  @just --choose

# Print this help text
help:
    @just --list

# Install project and python dependencies
install:
    uv sync --all-extras

# Initialize development environment (install deps + hooks)
init: install
    uv run prek install

# Update the baseline for detect-secrets
update-secrets:
    uv run detect-secrets scan > .secrets.baseline  # pragma: allowlist secret

# Run pytests with config from pyproject.toml
test:
    uv run pytest -c pyproject.toml

# Test and emit a coverage report
test-with-coverage:
    uv run pytest -c pyproject.toml --cov=./ --cov-report=xml

# Run ruff linter
lint:
    uv run ruff check .

# Format code with ruff
format:
    uv run ruff format .

# Check code formatting without modifying
format-check:
    uv run ruff format --check .

# Run security scans
security:
    uv run bandit -r . -c pyproject.toml
    uv run detect-secrets scan

# Remove temporary or build folders
clean:
    rm -rf build dist site *.egg-info
    find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf

# Tag as v$(<src>.__version__) and push to GH
tag:
    # Delete tag if it already exists
    git tag -d v{{VERSION}} || true
    # Tag and push
    git tag v{{VERSION}}

deploy-tag: tag
    git push origin v{{VERSION}}

deploy: deploy-tag

# Build the project
build: install clean
    uv run python -m build

# [DO NOT RUN - RUN VIA CICD] Build the project as a .pyz, so it and it's dependencies can be installed and imported with the orbiter binary
package:
  #  https://docs.python.org/3/library/zipapp.html#creating-standalone-applications-with-zipapp
  mkdir -p build
  uv pip install '.' --target build
  cp -r orbiter_translations build
  rm -rf build/*.dist-info/*
  rmdir build/*.dist-info
  uv run python -m zipapp \
    --compress \
    --main orbiter_translations.__main__:main \
    --python "/usr/bin/env python3" \
    --output dist/orbiter_translations.pyz \
    build
