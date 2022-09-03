#!/bin/bash
#!/bin/bash

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
  echo "Downloading package..."
  gsutil cp ${PACKAGE} .
  PACKAGENAME=$(basename ${PACKAGE})
  echo "Install package..."
  run_with_retry pip install --upgrade ${PACKAGENAME}
}

main
