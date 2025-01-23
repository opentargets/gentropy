#!/usr/bin/env bash

echo "Fetching version changes..."
git fetch
if output=$(git status --porcelain) && [[ -n "$output" ]]; then
    exit 1
fi
echo "Checking if branch is up-to-date with remote..."
if output=$(git push --dry-run --porcelain) && [[ "$output" != *"[up to date]"* ]]; then
    exit 1
fi
