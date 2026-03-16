#!/usr/bin/env bash
set -euo pipefail


SRA_VERSION="3.1.0"
ARCHIVE="sratoolkit.${SRA_VERSION}-ubuntu64.tar.gz"
TOOLKIT_DIR="sratoolkit.${SRA_VERSION}-ubuntu64"
URL="https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/${SRA_VERSION}/${ARCHIVE}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="${SCRIPT_DIR}"
BIN_DIR="${INSTALL_DIR}/${TOOLKIT_DIR}/bin"

persist_path="false"
if [[ "${1:-}" == "--persist" ]]; then
  persist_path="true"
fi

is_sourced="false"
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  is_sourced="true"
fi

echo "[1/4] Preparing download in: ${INSTALL_DIR}"
cd "${INSTALL_DIR}"

echo "[2/4] Downloading ${ARCHIVE}"
if [[ ! -f "${ARCHIVE}" ]]; then
  if command -v wget >/dev/null 2>&1; then
    wget "${URL}"
  elif command -v curl >/dev/null 2>&1; then
    curl -L -o "${ARCHIVE}" "${URL}"
  else
    echo "Error: neither wget nor curl is installed."
    exit 1
  fi
else
  echo "Archive already exists, skipping download."
fi

echo "[3/4] Extracting ${ARCHIVE}"
if [[ ! -d "${TOOLKIT_DIR}" ]]; then
  tar -xzf "${ARCHIVE}"
else
  echo "Toolkit directory already exists, skipping extraction."
fi

if [[ ! -x "${BIN_DIR}/prefetch" ]]; then
  echo "Error: prefetch not found at ${BIN_DIR}/prefetch"
  exit 1
fi

echo "[4/5] Configuring PATH"
export PATH="${BIN_DIR}:${PATH}"

if [[ "${is_sourced}" == "true" ]]; then
  echo "PATH updated for this shell session."
else
  echo "PATH was updated inside this script process."
  echo "Run with 'source setup_sratools.sh' to update your current shell PATH."
fi
echo "To keep this PATH in future shells, run:"
echo "  source setup_sratools.sh --persist"

echo "[5/5] Integrating with virtualenv (if active)"
venv_path="${VIRTUAL_ENV:-}"
# Defensive cleanup in case env value has trailing newlines from shell startup scripts.
venv_path="$(printf '%s' "${venv_path}" | tr -d '\r\n')"

if [[ -n "${venv_path}" ]] && [[ -d "${venv_path}/bin" ]]; then
  # Symlink SRA binaries into venv/bin so they are available without sourcing.
  linked_count=0
  for exe_path in "${BIN_DIR}"/*; do
    [[ -e "${exe_path}" ]] || continue
    [[ -x "${exe_path}" ]] || continue
    exe_name="$(basename "${exe_path}")"
    ln -sf "${exe_path}" "${venv_path}/bin/${exe_name}"
    linked_count=$((linked_count + 1))
  done

  echo "Linked ${linked_count} executable(s) into ${venv_path}/bin"
else
  echo "No active virtualenv detected; skipping venv link step."
fi

if [[ "${persist_path}" == "true" ]]; then
  line="export PATH=\"${BIN_DIR}:\$PATH\""
  if [[ -f "${HOME}/.bashrc" ]] && grep -Fq "${BIN_DIR}" "${HOME}/.bashrc"; then
    echo "PATH entry already present in ~/.bashrc"
  else
    echo "${line}" >> "${HOME}/.bashrc"
    echo "Added PATH entry to ~/.bashrc"
  fi
fi

echo
echo "Verifying installation:"
prefetch --version

echo
echo "SRA Toolkit setup complete."
