#!/usr/bin/env bash

set -exo pipefail

readonly GENTROPY_REF=$(/usr/share/google/get_metadata_value attributes/GENTROPY_REF || true)
readonly REPO_URI="https://github.com/opentargets/gentropy"
function err() {
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
    exit 1
}

function run_with_retry() {
    local -r cmd=("$@")
    for ((i = 0; i < 3; i++)); do
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
    echo "export HYDRA_FULL_ERROR=1" | tee --append /etc/profile
    source /etc/profile

    if [[ -z "${GENTROPY_REF}" ]]; then
        echo "ERROR: Must specify GENTROPY_REF metadata key"
        exit 1
    fi
    install_pip
    pip install uv

    pip uninstall -y gentropy
    echo "Install package..."
    run_with_retry uv pip install --no-break-system-packages --system "gentropy @ git+${REPO_URI}.git@${GENTROPY_REF}"
}

main
