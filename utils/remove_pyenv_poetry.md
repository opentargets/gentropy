In order to completely remove Pyenv and Poetry from the system, use these steps:

1. Uninstall Poetry: `curl -sSL https://install.python-poetry.org | python3 - --uninstall`
2. Clear Poetry cache: `rm -rf ~/.cache/pypoetry`
3. Switch to system Python shell: `pyenv shell system`
4. Edit `~/.bashrc` to remove the lines related to Pyenv configuration
5. Remove Pyenv configuration and cache: `rm -rf ~/.pyenv`
