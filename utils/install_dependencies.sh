export SHELL_RC=$(echo "$HOME/.${SHELL##*/}rc")
readonly PYTHON_VERSION=$(cat .python-version >&/dev/null || echo "3.11.11")

if ! command -v uv &>/dev/null; then
    echo "uv was not found, installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.local/bin/env
fi

echo "Installing python version from .python-version..."
uv python install $PYTHON_VERSION

echo "Installing dependencies with UV..."
uv sync --all-groups

echo "Setting up pre-commit..."
uv run pre-commit install
uv run pre-commit install --hook-type commit-msg
