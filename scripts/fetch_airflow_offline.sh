#!/usr/bin/env bash
# Pre-fetch large Python packages for the airflow image build.
#
# The airflow image installs pyspark, which PyPI only ships as a ~450 MB source
# tarball. On a slow or unstable link `pip install` inside `docker build`
# restarts that download from zero every time it drops. This script downloads it
# once on the host with resume support so the build consumes a local copy via
# `pip install --find-links docker/airflow/offline`.
#
# Safe to re-run: a partial file resumes, a complete+valid file is left alone,
# and an over-appended or corrupt file is reset and re-fetched.

set -euo pipefail

DEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/docker/airflow/offline"
mkdir -p "${DEST_DIR}"

# pyspark==4.2.0 sdist, pinned by digest path so the URL is stable.
PYSPARK_URL="https://files.pythonhosted.org/packages/c3/33/c987434f5d50aa802779a004ca0fd45ee4350caab50554ad7283d5a22b50/pyspark-4.2.0.tar.gz"
PYSPARK_SIZE=450129423
PYSPARK_FILE="${DEST_DIR}/pyspark-4.2.0.tar.gz"

file_size() {
  stat -c %s "$1" 2>/dev/null || stat -f %z "$1" 2>/dev/null || echo 0
}

for attempt in $(seq 1 80); do
  size=$(file_size "${PYSPARK_FILE}")

  if [ "${size}" -gt "${PYSPARK_SIZE}" ]; then
    echo "attempt ${attempt}: file over-appended (${size}); resetting"
    rm -f "${PYSPARK_FILE}"
    size=0
  fi

  if [ "${size}" = "${PYSPARK_SIZE}" ]; then
    if gzip -t "${PYSPARK_FILE}" 2>/dev/null; then
      echo "pyspark-4.2.0.tar.gz complete and valid"
      exit 0
    fi
    echo "attempt ${attempt}: right size but corrupt; resetting"
    rm -f "${PYSPARK_FILE}"
  fi

  # -C - resumes a 206-capable server; no --retry-all-errors so an HTTP error
  # is never turned into a full-body re-request that appends.
  curl -L -C - --retry 5 --retry-delay 5 --connect-timeout 30 \
    -o "${PYSPARK_FILE}" "${PYSPARK_URL}" || true

  echo "attempt ${attempt}: $(file_size "${PYSPARK_FILE}")/${PYSPARK_SIZE} bytes"
  sleep 3
done

echo "failed to download a valid pyspark-4.2.0.tar.gz after 80 attempts" >&2
exit 1
