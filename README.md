# American Beauty Institute Data Platform

<h1 align="center">
  <img
    src="https://raw.githubusercontent.com/americanbeautyinstitute/data-platform/main/docs/assets/logo.png" alt="American Beauty Institute Logo">
  <br>
</h1>

[![CI](https://github.com/AmericanBeautyInstitute/data-platform/actions/workflows/ci.yaml/badge.svg)](https://github.com/AmericanBeautyInstitute/data-platform/actions)
[![codecov](https://codecov.io/gh/AmericanBeautyInstitute/data-platform/graph/badge.svg?token=GZ8WN0KY9P)](https://codecov.io/gh/AmericanBeautyInstitute/data-platform)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)

---

**Documentation:** [americanbeautyinstitute.pages.dev](americanbeautyinstitute.pages.dev)

**Source Code:** [https://github.com/AmericanBeautyInstitute](https://github.com/AmericanBeautyInstitute)

---

Repository for all data related code for American Beauty Institute.

# Development environment setup

It is recommended to use this project's [Dev Container](https://code.visualstudio.com/docs/devcontainers/containers) setup which handles all the configurations and installations needed to run the platform. Install the Dev Container VSCode extension and run `Dev Containers: Open Folder in Container...` under the command palette.

Alternatively, [uv](https://docs.astral.sh/uv/getting-started/installation/) can be used directly without the Dev Container, though [Terraform](https://developer.hashicorp.com/terraform/install) and [Google Cloud CLI](https://docs.cloud.google.com/sdk/docs/install-sdk) will need to be installed separately for deployment and infrastructural tasks.

```bash
git clone https://github.com/AmericanBeautyInstitute/data-platform.git
cd data-platform
make setup
```