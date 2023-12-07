#!/bin/bash

# Install Poetry environment
poetry install --no-interaction --no-root --no-cache

# Install Pre-commit hooks
poetry run pre-commit install --install-hooks
