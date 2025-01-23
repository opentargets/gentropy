#!/usr/bin/env bash

echo "Fetching version changes..."
git fetch
if output=$(git status --porcelain) && [ -z "$output" ]; then
    echo "Checking if branch is up-to-date with remote..."
elif output=$(git push --dry-run --porcelain) && [[ "$output" == "* up to date *" ]]; then
    exit 0
else
    exit 1
fi
