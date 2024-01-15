#!/usr/bin/env bash

set -exo pipefail

readonly PACKAGE=$(/usr/share/google/get_metadata_value attributes/PACKAGE || true)
readonly CONFIGTAR=$(/usr/share/google/get_metadata_value attributes/CONFIGTAR || true)

function err() {
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
    exit 1
}

function run_with_retry() {
    local -r cmd=("$@")
    for ((i = 0; i < 10; i++)); do
        if "${cmd[@]}"; then
            return 0
        fi
        sleep 5
    done
    err "Failed to run command: ${cmd[*]}"
}

function install_pip() {
    if command -v pip >/dev/null; then
        echo "pip is already installed."
        return 0
    fi

    if command -v easy_install >/dev/null; then
        echo "Installing pip with easy_install..."
        run_with_retry easy_install pip
        return 0
    fi

    echo "Installing python-pip..."
    run_with_retry apt update
    run_with_retry apt install python-pip -y
}

function main() {
    # Define a specific directory to download the files
    local work_dir="/"
    cd "${work_dir}" || err "Failed to change to working directory"
    echo "Working directory: $(pwd)"

    # more meaningful errors from hydra
    echo "export HYDRA_FULL_ERROR=1" | tee --append /etc/profile
    source /etc/profile

    if [[ -z "${PACKAGE}" ]]; then
        echo "ERROR: Must specify PACKAGE metadata key"
        exit 1
    fi
    install_pip

    echo "Downloading package..."
    gsutil cp ${PACKAGE} . || err "Failed to download PACKAGE"
    PACKAGENAME=$(basename ${PACKAGE})

    echo "Uninstalling previous version if it exists"
    pip uninstall -y oxygen
    echo "Install package..."
    run_with_retry pip install --upgrade ${PACKAGENAME}

    echo "Downloading and uncompressing config..."
    gsutil cp ${CONFIGTAR} . || err "Failed to download CONFIGTAR"
    tar -xvf $(basename ${CONFIGTAR}) || err "Failed to extract CONFIGTAR"
}

main
