#!/usr/bin/env bash

set -euo pipefail
set -x

GENTROPY_REF=$(/usr/share/google/get_metadata_value attributes/GENTROPY_REF || true)
readonly GENTROPY_REF
readonly REPO_URI="https://github.com/opentargets/gentropy"
readonly UV_VERSION="0.12.24"

function err() {
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
    exit 1
}

function run_with_retry() {
    local -r cmd=("$@")
    local delay=5
    for ((i = 0; i < 5; i++)); do
        if "${cmd[@]}"; then
            return 0
        fi
        sleep "${delay}"
        delay=$((delay * 2))
    done
    err "Failed to run command: ${cmd[*]}"
}

# The interpreter PySpark jobs run under. Dataproc exports it as PYSPARK_PYTHON
# from /etc/profile.d/effective-python.sh, on the 2.x (conda) and 3.0 (Pixi)
# images alike, so nothing here depends on where the image keeps its Python.
# https://cloud.google.com/dataproc/docs/tutorials/python-configuration
function spark_python() {
    set +u
    # shellcheck disable=SC1091
    source /etc/profile.d/effective-python.sh
    set -u
    if [[ -z "${PYSPARK_PYTHON:-}" || ! -x "${PYSPARK_PYTHON}" ]]; then
        err "PYSPARK_PYTHON is not set to an executable by /etc/profile.d/effective-python.sh"
    fi
    echo "${PYSPARK_PYTHON}"
}

function main() {
    if [[ -z "${GENTROPY_REF}" ]]; then
        err "Must specify GENTROPY_REF metadata key"
    fi
    echo "export HYDRA_FULL_ERROR=1" | tee --append /etc/profile

    local python
    python=$(spark_python)
    "${python}" --version

    # uv goes into the same environment, then installs into it explicitly with
    # --python, rather than into whichever interpreter is first on PATH.
    if ! "${python}" -m pip --version; then
        "${python}" -m ensurepip
    fi
    run_with_retry "${python}" -m pip install "uv==${UV_VERSION}"

    # Temporary: a GitHub HTTP/2 defect makes unauthenticated fetches fail on
    # some git builds -- the ref advertisement returns 200, the pack negotiation
    # 401. Covers the fetch below. Remove once GitHub resolves it.
    # https://github.com/orgs/community/discussions/206581
    git config --system http.version HTTP/1.1

    local src
    src=$(mktemp -d)
    git -C "${src}" init -q
    git -C "${src}" remote add origin "${REPO_URI}.git"
    run_with_retry git -C "${src}" fetch -q --depth 1 origin "${GENTROPY_REF}"
    git -C "${src}" checkout -q FETCH_HEAD
    git -C "${src}" log -1 --format='gentropy source: %H %s'

    # The locked dependencies, but pyspark and py4j come from the image's Spark.
    "${python}" -m uv export --quiet --project "${src}" --frozen --no-dev --no-emit-project \
        --format pylock.toml --no-emit-package pyspark --no-emit-package py4j \
        --output-file "${src}/pylock.toml"

    "${python}" -m uv pip uninstall --python "${python}" gentropy || true
    echo "Install package..."
    run_with_retry "${python}" -m uv pip install --python "${python}" --no-break-system-packages \
        --preview-features pylock -r "${src}/pylock.toml"
    run_with_retry "${python}" -m uv pip install --python "${python}" --no-break-system-packages \
        --no-deps "${src}"
    # Report only: packages the image ships may declare pins the lock does not meet.
    "${python}" -m uv pip check --python "${python}" || true
    "${python}" -c 'import pyspark, hail; print("pyspark", pyspark.__version__, "hail", hail.version())'
}

main
