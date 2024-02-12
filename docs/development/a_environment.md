# Development environment

## Devcontainer (recommended)

Developing in a devcontainer ensures a reproducible development environment with minimal setup. To use the devcontainer, you need to have Docker installed on your system and use Visual Studio Code as your IDE.

For a quick start you can either:

1. [Open existing folder in container](https://code.visualstudio.com/docs/devcontainers/containers#_quick-start-open-an-existing-folder-in-a-container).

1. [Clone repository in container volume](https://code.visualstudio.com/docs/devcontainers/containers#_quick-start-open-a-git-repository-or-github-pr-in-an-isolated-container-volume).

[![Open in Dev Containers](https://img.shields.io/static/v1?label=Dev%20Containers&message=Open&color=blue&logo=visualstudiocode)](https://vscode.dev/redirect?url=vscode://ms-vscode-remote.remote-containers/cloneInVolume?url=https://github.com/opentargets/gentropy)

If you already have VS Code and Docker installed, you can click the badge above or [here](https://vscode.dev/redirect?url=vscode://ms-vscode-remote.remote-containers/cloneInVolume?url=https://github.com/opentargets/gentropy) to get started. Clicking these links will cause VS Code to automatically install the Dev Containers extension if needed, clone the source code into a container volume, and spin up a dev container for use.

More information on working with devcontainers can be found [here](https://code.visualstudio.com/docs/devcontainers/containers).

## Codespaces

A devcontainer can also be triggered within Github using [Codespaces](https://github.com/features/codespaces). This option requires no local setup as the environment is managed by Github.

## One-time configuration

To setup a full local environment of the package please follow the next steps.

Requirements:

- java
- make
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install).

Google Cloud configuration:

1.  Log in to your work Google Account: run `gcloud auth login` and follow instructions.

1.  Obtain Google application credentials: run `gcloud auth application-default login` and follow instructions.

Finally, run `make setup-dev` to install/update the necessary packages and activate the development environment.
