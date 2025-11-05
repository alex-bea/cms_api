#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'USAGE'
Bootstrap the CMS Pricing API development environment.

Usage:
  scripts/bootstrap_env.sh [--force-recreate] [--skip-native] [--python-bin <python>]

Options:
  --force-recreate   Remove and recreate the .venv directory even if it exists.
  --skip-native      Skip Homebrew/apt installs (assume native deps already present).
  --python-bin       Override the Python interpreter used to create the venv (default: python3.11).
USAGE
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FORCE_RECREATE=0
SKIP_NATIVE=0
PYTHON_BIN="${PYTHON_BIN:-python3.11}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force-recreate)
      FORCE_RECREATE=1
      shift
      ;;
    --skip-native)
      SKIP_NATIVE=1
      shift
      ;;
    --python-bin)
      PYTHON_BIN="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

log() {
  printf "[bootstrap] %s\n" "$*"
}

ensure_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

install_native_macos() {
  ensure_cmd brew
  local formulas=(
    python@3.11
    apache-arrow
    snappy
    tesseract
    libomp
  )
  for formula in "${formulas[@]}"; do
    if brew ls --versions "$formula" >/dev/null 2>&1; then
      log "Homebrew formula already installed: $formula"
    else
      log "Installing $formula via Homebrew"
      brew install "$formula"
    fi
  done
  log "Homebrew versions:"
  brew ls --versions "${formulas[@]}"
}

install_native_linux() {
  ensure_cmd sudo
  ensure_cmd apt-get
  log "Updating apt package index"
  sudo apt-get update
  log "Installing native libraries via apt"
  sudo apt-get install -y python3.11 python3.11-venv libarrow-dev libparquet-dev libtesseract-dev tesseract-ocr libsnappy-dev
}

install_native_deps() {
  if [[ "$SKIP_NATIVE" -eq 1 ]]; then
    log "Skipping native dependency installation (--skip-native)"
    return
  fi

  case "$(uname -s)" in
    Darwin)
      install_native_macos
      ;;
    Linux)
      install_native_linux
      ;;
    *)
      echo "Unsupported OS: $(uname -s)" >&2
      exit 1
      ;;
  esac
}

create_virtualenv() {
  if [[ "$FORCE_RECREATE" -eq 1 && -d "${ROOT}/.venv" ]]; then
    log "Removing existing virtual environment (.venv)"
    rm -rf "${ROOT}/.venv"
  fi

  if [[ ! -d "${ROOT}/.venv" ]]; then
    log "Creating virtual environment with ${PYTHON_BIN}"
    ensure_cmd "$PYTHON_BIN"
    "$PYTHON_BIN" -m venv "${ROOT}/.venv"
  else
    log "Reusing existing virtual environment (.venv)"
  fi
}

install_python_packages() {
  local python="${ROOT}/.venv/bin/python"
  ensure_cmd "$python"

  log "Upgrading pip/setuptools/wheel"
  if ! "$python" -m pip install --upgrade pip setuptools wheel; then
    log "Unable to reach package index while upgrading pip tooling; continuing with existing versions"
  fi

  log "Installing runtime requirements"
  "$python" -m pip install -r "${ROOT}/requirements.txt"

  log "Installing development requirements"
  "$python" -m pip install -r "${ROOT}/requirements-dev.txt"

  if [[ -z "${CI:-}" ]]; then
    log "Freezing environment to requirements.lock"
    "$python" -m pip freeze > "${ROOT}/requirements.lock"
  else
    log "CI environment detected; skipping requirements.lock update"
  fi
}

install_native_deps
create_virtualenv
install_python_packages

log "Environment bootstrap complete. Activate it with: source .venv/bin/activate"
