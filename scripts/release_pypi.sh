#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

usage() {
  cat <<'EOF'
Usage:
  scripts/release_pypi.sh testpypi
  scripts/release_pypi.sh pypi
  scripts/release_pypi.sh all

Behavior:
  - loads .env from the repository root when present
  - rebuilds dist artifacts from scratch
  - runs twine checks before upload
  - uploads to TestPyPI, PyPI, or both

Required .env variables:
  TWINE_USERNAME=__token__
  TEST_PYPI_TOKEN=pypi-...   # for testpypi/all
  PYPI_TOKEN=pypi-...        # for pypi/all
EOF
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

TARGET="$1"

case "${TARGET}" in
  testpypi|pypi|all)
    ;;
  *)
    usage
    exit 1
    ;;
esac

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

TWINE_USERNAME="${TWINE_USERNAME:-__token__}"

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required environment variable: ${name}" >&2
    exit 1
  fi
}

require_command() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "Required command not found: ${name}" >&2
    exit 1
  fi
}

build_artifacts() {
  require_command python3

  echo "Cleaning old build artifacts"
  rm -rf "${ROOT_DIR}/dist" "${ROOT_DIR}/build" "${ROOT_DIR}"/*.egg-info

  echo "Building package"
  (
    cd "${ROOT_DIR}"
    python3 -m build --no-isolation
  )

  echo "Checking artifacts with twine"
  (
    cd "${ROOT_DIR}"
    python3 -m twine check dist/*
  )
}

upload_target() {
  local repository="$1"
  local password_var="$2"

  require_var "${password_var}"

  echo "Uploading to ${repository}"
  (
    cd "${ROOT_DIR}"
    TWINE_USERNAME="${TWINE_USERNAME}" \
    TWINE_PASSWORD="${!password_var}" \
    python3 -m twine upload --non-interactive --repository "${repository}" dist/*
  )
}

build_artifacts

case "${TARGET}" in
  testpypi)
    upload_target "testpypi" "TEST_PYPI_TOKEN"
    ;;
  pypi)
    upload_target "pypi" "PYPI_TOKEN"
    ;;
  all)
    upload_target "testpypi" "TEST_PYPI_TOKEN"
    upload_target "pypi" "PYPI_TOKEN"
    ;;
esac
