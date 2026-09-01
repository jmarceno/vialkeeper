#!/usr/bin/env bash
# Install or upgrade a VialKeeper OTP release on a co-located host.
# Usage: sudo ./deploy/install-release.sh <release-dir-or-tarball> [release-id]
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

SRC="${1:?release directory or tarball required}"
RELEASE_ID="${2:-$(date -u +%Y%m%dT%H%M%SZ)}"
RELEASES_ROOT="/opt/vial_keeper-releases"
TARGET="${RELEASES_ROOT}/${RELEASE_ID}"
LINK="/opt/vial_keeper"
ROOT="${VIAL_KEEPER_ROOT:-/var/lib/vialkeeper}"
UNIT_SRC="$(cd "$(dirname "$0")" && pwd)/vialkeeper.service"

install -d -m 0755 "${RELEASES_ROOT}"
install -d -o vialkeeper -g vialkeeper -m 0750 "${ROOT}"

if [[ -f "${SRC}" ]]; then
  mkdir -p "${TARGET}"
  tar -xzf "${SRC}" -C "${TARGET}"
elif [[ -d "${SRC}" ]]; then
  mkdir -p "${TARGET}"
  cp -a "${SRC}/." "${TARGET}/"
else
  echo "source not found: ${SRC}" >&2
  exit 1
fi

test -x "${TARGET}/bin/vial_keeper"

ln -sfn "${TARGET}" "${LINK}"
install -o root -g root -m 0644 "${UNIT_SRC}" /etc/systemd/system/vialkeeper.service
systemctl daemon-reload
systemctl enable vialkeeper.service
systemctl restart vialkeeper.service

# Health: list databases with bearer when auth is enabled.
# Operators must export VIALKEEPER_TOKEN in the environment for this check.
if [[ -n "${VIALKEEPER_TOKEN:-}" ]]; then
  curl -fsS -H "Authorization: Bearer ${VIALKEEPER_TOKEN}" \
    "http://127.0.0.1:4000/v1/databases" >/dev/null
fi

echo "installed release ${RELEASE_ID} at ${TARGET}"
