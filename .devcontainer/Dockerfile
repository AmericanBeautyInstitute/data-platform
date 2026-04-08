ARG VARIANT=3.13-trixie
FROM mcr.microsoft.com/devcontainers/python:${VARIANT}

RUN \
    # Add Terraform repository and GPG key
    curl -fsSL https://apt.releases.hashicorp.com/gpg \
        | gpg --dearmor -o /usr/share/keyrings/hashicorp.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/hashicorp.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
        > /etc/apt/sources.list.d/hashicorp.list \
    \
    # Add Google Cloud SDK repository and GPG key
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/google-cloud.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/google-cloud.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud.sdk.list \
    # Install Terraform and Google Cloud CLI
    && apt-get update \
    && apt-get install -y terraform google-cloud-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv