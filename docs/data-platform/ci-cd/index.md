---
title: CI/CD
---

Continuous integration runs on every push and pull request via GitHub Actions. There is no continuous deployment — production deploys are triggered manually with `make deploy`.

## Pipeline

The CI workflow (`.github/workflows/ci.yaml`) runs a single `test` job across a matrix of:

- **OS:** Ubuntu, macOS
- **Python:** 3.11, 3.12, 3.13

Each matrix entry runs the following steps:

1. Check out the repository.
2. Set up the target Python version.
3. Install `uv`.
4. Run `make build`, which executes in order:
    - `uv sync --all-groups` — install all dependencies
    - `make reformat` — run Ruff formatter
    - `make lint` — run Ruff linter with auto-fix
    - `make type_check` — run ty type checker
    - `make docs` — build the MkDocs site
    - `make test` — run pytest with coverage
5. Generate a coverage report.
6. Upload coverage to Codecov.

The matrix uses `fail-fast: false`, so all combinations run to completion even if one fails.

## Coverage

Coverage is collected by pytest-cov with branch coverage enabled. Reports are uploaded to Codecov with the `unittests` flag, tagged by OS and Python version.

The current `fail_under` threshold is `0` (no minimum enforced).

## Pre-commit Hooks

Locally, pre-commit runs Ruff check and format on every commit. CI runs the same checks as part of `make build`, so any formatting or lint issues caught locally will also fail in CI.
