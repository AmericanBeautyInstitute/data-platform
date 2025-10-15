# See here for image contents: https://github.com/devcontainers/templates/tree/main/src/ubuntu

FROM mcr.microsoft.com/devcontainers/base:jammy

RUN \
    apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y bash-completion curl

# Coding only for vscode user and not root
USER vscode

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
