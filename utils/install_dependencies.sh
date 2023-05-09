if ! command -v pyenv &>/dev/null; then
    echo "Installing Pyenv, a tool to manage multiple Python versions..."
    curl -sSL https://pyenv.run | bash
    # Add Pyenv configuration to ~/.bashrc.
    echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
    echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
    echo 'eval "$(pyenv init -)"' >> ~/.bashrc
    # And also execute it right now.
    . <(tail -n3 ~/.bashrc)
fi

echo "Activating Pyenv environment with a Python version required for the project..."
PYTHON_VERSION=$(grep '^python = ".*"' pyproject.toml | cut -d'"' -f2)
pyenv install --skip-existing $PYTHON_VERSION
pyenv shell $PYTHON_VERSION

if ! command -v poetry &>/dev/null; then
    echo "Installing Poetry, a tool to manage Python dependencies..."
    curl -sSL https://install.python-poetry.org | python3 -
fi

echo "Preparing the Poetry environment and installing dependencies..."
poetry env use $PYTHON_VERSION
poetry install --remove-untracked

echo "Setting up pre-commit..."
poetry run pre-commit install
poetry run pre-commit autoupdate
poetry run pre-commit install --hook-type commit-msg

echo "Activating the Poetry environment..."
poetry shell
