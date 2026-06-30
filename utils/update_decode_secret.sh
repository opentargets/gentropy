#!/usr/bin/env bash
# Updates the 'decode' GCP secret with the contents of a JSON file matching the s3_config schema:
# { bucket_name, access_key_id, secret_access_key, s3_host_url, s3_host_port }
set -euo pipefail

SECRET_NAME="decode"

usage() {
    echo "Usage: $0 <path-to-s3-config.json>"
    echo ""
    echo "  <path-to-s3-config.json>  JSON file with keys: bucket_name, access_key_id,"
    echo "                            secret_access_key, s3_host_url, s3_host_port"
    exit 1
}

[[ $# -ne 1 ]] && usage
CONFIG_FILE="$1"

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: file not found: $CONFIG_FILE" >&2
    exit 1
fi

# Validate required keys are present
for key in bucket_name access_key_id secret_access_key s3_host_url s3_host_port; do
    if ! jq -e --arg k "$key" 'has($k)' "$CONFIG_FILE" > /dev/null 2>&1; then
        echo "Error: missing required key '$key' in $CONFIG_FILE" >&2
        exit 1
    fi
done

gcloud secrets versions add "$SECRET_NAME" --data-file="$CONFIG_FILE"

echo "Secret '$SECRET_NAME' updated successfully."
