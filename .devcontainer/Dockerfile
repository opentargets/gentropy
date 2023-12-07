# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.231.6/containers/python-3/.devcontainer/base.Dockerfile

# [Choice] Python version (use -bullseye variants on local arm64/Apple Silicon): 3, 3.10, 3.9, 3.8, 3.7, 3.6, 3-bullseye, 3.10-bullseye, 3.9-bullseye, 3.8-bullseye, 3.7-bullseye, 3.6-bullseye, 3-buster, 3.10-buster, 3.9-buster, 3.8-buster, 3.7-buster, 3.6-buster
ARG VARIANT="3.10-bullseye"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

ENV POETRY_HOME="/home/vscode/.poetry"
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"
ENV POETRY_VERSION=1.7.1

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    htop \
    fzf \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock ./
RUN pipx install poetry==${POETRY_VERSION}

# Add fzf to .zshrc
RUN echo "source /usr/share/doc/fzf/examples/key-bindings.zsh" >> /home/vscode/.zshrc
RUN echo "source /usr/share/doc/fzf/examples/completion.zsh" >> /home/vscode/.zshrc
