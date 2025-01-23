#!/usr/bin/env bash

echo "Fetching version changes..."
git fetch
if output=$(git status --porcelain) && [ -z "$output" ]; then
    exit 0
else
    exit 1
fi
