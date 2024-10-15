#!/usr/bin/env bash
export SHELL_RC=$(echo "$HOME/.${SHELL##*/}rc")
if ! command -v pyenv &>/dev/null; then
    echo "Installing Pyenv, a tool to manage multiple Python versions..."
    curl -sSL https://pyenv.run | bash
    # Add Pyenv configuration to rc.
    echo 'export PYENV_ROOT="$HOME/.pyenv"' >>$SHELL_RC
    echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >>$SHELL_RC
    echo 'eval "$(pyenv init -)"' >>$SHELL_RC
    # And also execute it right now.
    . <(tail -n3 $SHELL_RC)
fi

echo "Activating Pyenv environment with a Python version required for the project..."
if [ -f ".python-version" ]; then
    PYTHON_VERSION=$(cat .python-version)
    pyenv install --skip-existing $PYTHON_VERSION
    pyenv shell $PYTHON_VERSION
else
    echo ".python-version file not found."
    exit 1
fi

if ! command -v poetry &>/dev/null; then
    echo "Installing Poetry, a tool to manage Python dependencies..."
    curl -sSL https://install.python-poetry.org | python3 -
fi

echo "Preparing the Poetry environment and installing dependencies..."
poetry env use $PYTHON_VERSION
poetry install --sync

echo "Setting up pre-commit..."
poetry run pre-commit install
poetry run pre-commit install --hook-type commit-msg

echo "Activating the Poetry environment..."
poetry shell
