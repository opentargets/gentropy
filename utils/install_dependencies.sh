readonly SHELL_RC="$HOME/.${SHELL##*/}rc"
readonly PYTHON_VERSION=$(cat .python-version >&/dev/null || echo "3.11.11")


if ! command -v uv &>/dev/null; then
	echo "uv was not found, installing uv..."
	curl -LsSf https://astral.sh/uv/install.sh | sh
	# UV installs to XDG_BIN_HOME, XDG_DATA_HOME or to the $HOME/.local/bin
	echo "Looking for uv installation path..."
	if [[ -n "${XDG_BIN_HOME}" ]]; then
	  install_path="$XDG_BIN_HOME"
	elif [[ -n "${XDG_DATA_HOME}" ]]; then
	  install_path="$XDG_DATA_HOME"
	else
	  install_path="${HOME}/.local/bin"
	fi
	echo "uv was installed under ${install_path}"
  # Ensure the _install_path is available under the $PATH
  echo "Ensuring that uv is ready in the environment..."
  if [[ -n $(echo "${PATH//:/\n}" | grep "${install_path}") ]]; then
	  echo "Environment ready";
	else
	  # If the _install_path is exported to in the rc file, just source it,
	  # otherwise append it to the rc file
	  expression="export PATH=\"${install_path}:\$PATH\""
	  if [[ -z $(grep "${expression}" "${SHELL_RC}") ]]; then
	    echo "Exporting ${install_path} to PATH in ${SHELL_RC}..."
	    echo ${expression} >> "$SHELL_RC"
	  fi
  fi
fi

# shellcheck source=/dev/null
source "$SHELL_RC"
echo "Installing python version from .python-version..."
uv python install "$PYTHON_VERSION"

echo "Installing dependencies with UV..."
uv sync --all-groups

echo "Setting up pre-commit..."
uv run pre-commit install
uv run pre-commit install --hook-type commit-msg
