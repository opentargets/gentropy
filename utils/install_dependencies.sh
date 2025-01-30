readonly SHELL_RC="$HOME/.${SHELL##*/}rc"
readonly PYTHON_VERSION=$(cat .python-version >&/dev/null || echo "3.11.11")

if ! command -v uv &>/dev/null; then
    echo "uv was not found, installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source "$SHELL_RC"
fi

echo "Installing python version from .python-version..."
uv python install "$PYTHON_VERSION"

echo "Installing dependencies with UV..."
uv sync --all-groups --frozen

echo "Setting up pre-commit..."
uv run --no-sync pre-commit install
uv run --no-sync pre-commit install --hook-type commit-msg
