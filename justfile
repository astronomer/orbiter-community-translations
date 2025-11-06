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

# Install project and python dependencies (incl. pre-commit) locally
install EDITABLE='':
    {{PYTHON}} -m pip install {{EDITABLE}} '.[{{EXTRAS}}]'

# Install pre-commit to local project
install-precommit: install
    pre-commit install

# Update the baseline for detect-secrets / pre-commit
update-secrets:
    detect-secrets scan  > .secrets.baseline  # pragma: allowlist secret

# Run pytests with config from pyproject.toml
test:
    {{PYTHON}} -m pytest -c pyproject.toml

# Test and emit a coverage report
test-with-coverage:
    {{PYTHON}} -m pytest -c pyproject.toml --cov=./ --cov-report=xml

# Run ruff and black (normally done with pre-commit)
lint:
    ruff check .

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
    {{PYTHON}} -m build

# [DO NOT RUN - RUN VIA CICD] Build the project as a .pyz, so it and it's dependencies can be installed and imported with the orbiter binary
package:
  #  https://docs.python.org/3/library/zipapp.html#creating-standalone-applications-with-zipapp
  mkdir -p build
  {{PYTHON}} -m pip install '.' --target build
  cp -r orbiter_translations build
  rm -rf build/*.dist-info/*
  rmdir build/*.dist-info
  {{PYTHON}} -m zipapp \
    --compress \
    --main orbiter_translations.__main__:main \
    --python "/usr/bin/env python3" \
    --output dist/orbiter_translations.pyz \
    build
