---
title: Development Setup
---

## Prerequisites

- Python 3.11, 3.12, or 3.13
- [uv](https://docs.astral.sh/uv/) package manager
- A `.env` file with valid credentials (see [Authentication](../data-ingestion/authentication.md))

## Quick Start

Clone the repo and run the setup target:

```bash
git clone https://github.com/americanbeautyinstitute/data-platform.git
cd data-platform
make setup
```

This installs all dependencies (including dev groups) and sets up pre-commit hooks.

Then create your `.env` file:

```bash
cp .env.example .env
# Fill in credential values
```

## Devcontainer

The repo includes a VS Code devcontainer (`.devcontainer/`) that comes pre-installed with:

- **uv** — Python package manager
- **Terraform** — infrastructure management
- **Google Cloud CLI** — `gcloud` for GCP operations

To use it, open the repo in VS Code and select **Reopen in Container** when prompted. The container runs `make setup` automatically on creation.

The devcontainer also configures VS Code with:

- Ruff as the default formatter with format-on-save
- pytest discovery from the `tests/` directory
- Extensions: Ruff, Python, Pylance, YAML, Cucumber

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make setup` | Install all dependencies and pre-commit hooks |
| `make build` | Full build: sync, reformat, lint, type check, docs, test |
| `make test` | Run pytest with coverage |
| `make lint` | Run Ruff linter with auto-fix |
| `make reformat` | Run Ruff formatter |
| `make type_check` | Run ty type checker on tests |
| `make docs` | Build MkDocs site |
| `make serve` | Serve docs locally |
| `make ssh` | SSH into the production VM |
| `make deploy` | Deploy latest code to production |

## Pre-commit Hooks

Pre-commit runs two hooks on every commit:

1. **ruff-check** — lints and auto-fixes issues
2. **ruff-format** — formats code

These are installed by `make setup`. To run manually:

```bash
uv run pre-commit run --all-files
```

## Running Tests

```bash
make test
```

This runs pytest with coverage enabled. Tests live in the `tests/` directory. Coverage is configured with `branch = true` and reports missing lines.

## Project Structure

```
data-platform/
├── assets/              # Dagster asset definitions and resources
├── extract/             # Source-specific extractor packages
├── load/                # GCS and BigQuery loader modules
├── transform/           # SQLMesh models and configuration
├── infra/               # Terraform and VM configuration files
├── tests/               # Test suite
├── docs/                # MkDocs documentation source
├── .devcontainer/       # VS Code devcontainer configuration
├── .github/workflows/   # CI pipeline
├── pyproject.toml       # Project metadata and dependencies
├── Makefile             # Build, test, and deploy targets
└── .env.example         # Environment variable template
```
