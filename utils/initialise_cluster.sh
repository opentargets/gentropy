#!/usr/bin/env bash

set -exo pipefail

readonly PACKAGE=$(/usr/share/google/get_metadata_value attributes/PACKAGE || true)

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

# Some functionalities might missing from the vanilla dataproc instances:
# TODO: it is possible to test if the jars are compiled with the suitable spark and scala version... we should consider testing for it. Otherwise we might get surprises in the future.
function install_custom_jars(){

    # List of compiled spark packages to install on dataproc:
    declare -a jars_to_install=(
        'https://repos.spark-packages.org/graphframes/graphframes/0.8.0-spark3.0-s_2.12/graphframes-0.8.0-spark3.0-s_2.12.jar'
    )

    # Jars directory on dataproc:
    JARS_DIR=${SPARK_HOME}/jars

    # Testing if this directory exists:
    if [[ -z ${SPARK_HOME} ]]; then
        err "SPARK_HOME variable is not set. Installing custom jars failed."
    elif [[ ! -d ${JARS_DIR} ]]; then
        err "Jars directory (${JARS_DIR}) does not exists. Installing custom jars failed."
    fi
    echo "Installing custom jars to: ${JARS_DIR}"

    for jar in ${jars_to_install[@]}; do
        echo "Installing $jar..."
        wget -q "${jar}" -P "${JARS_DIR}"
    done
    echo "Custom jar installation finished."
    return 0
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
    if [[ -z "${PACKAGE}" ]]; then
        echo "ERROR: Must specify PACKAGE metadata key"
        exit 1
    fi
    install_pip
    install_custom_jars
    echo "Downloading package..."
    gsutil cp ${PACKAGE} .
    PACKAGENAME=$(basename ${PACKAGE})
    echo "Install package..."
    run_with_retry pip install --upgrade ${PACKAGENAME}
}

main
