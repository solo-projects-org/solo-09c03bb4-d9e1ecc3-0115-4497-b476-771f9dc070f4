#!/usr/bin/env bash
# 容器入口：按需生成样例数据，然后 exec 主进程
set -euo pipefail

ARCHIVE_DIR="${PM25_ARCHIVE_DIR:-/data/archive}"
MANIFEST_PATH="${PM25_MANIFEST_PATH:-/data/manifest/manifest.json}"

if [[ "${GENERATE_SAMPLE:-0}" == "1" ]]; then
  if [[ -z "$(find "$ARCHIVE_DIR" -name '*.parquet' -print -quit 2>/dev/null || true)" ]]; then
    echo "[entrypoint] 档案目录为空，生成样例 Parquet 数据..."
    PYTHONPATH=/app/src python /app/scripts/generate_sample.py \
      --archive "$ARCHIVE_DIR" --manifest "$MANIFEST_PATH"
  else
    echo "[entrypoint] 档案目录已有数据，跳过样例生成"
  fi
fi

exec "$@"
