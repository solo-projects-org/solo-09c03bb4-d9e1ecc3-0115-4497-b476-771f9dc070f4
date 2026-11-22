#!/usr/bin/env bash
# 容器入口：按需生成样例数据，然后 exec 主进程
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

ARCHIVE_DIR="${PM25_ARCHIVE_DIR:-/data/archive}"
MANIFEST_PATH="${PM25_MANIFEST_PATH:-/data/manifest/manifest.json}"

if [[ "${GENERATE_SAMPLE:-0}" == "1" ]]; then
  if [[ -z "$(find "$ARCHIVE_DIR" -name '*.parquet' -print -quit 2>/dev/null || true)" ]]; then
    echo "[entrypoint] 档案目录为空，生成样例 Parquet 数据..."
    PYTHONPATH="$APP_DIR/src" python "$SCRIPT_DIR/generate_sample.py" \
      --archive "$ARCHIVE_DIR" --manifest "$MANIFEST_PATH"
  elif [[ ! -f "$MANIFEST_PATH" ]]; then
    # 有档案但清单缺失（例如首次切换到清单模式的旧卷）：按现有文件补建清单
    echo "[entrypoint] 清单缺失，按现有档案补建 $MANIFEST_PATH"
    PYTHONPATH="$APP_DIR/src" python "$SCRIPT_DIR/generate_sample.py" \
      --archive "$ARCHIVE_DIR" --manifest "$MANIFEST_PATH" --manifest-only
  else
    echo "[entrypoint] 档案与清单已有数据，跳过样例生成"
  fi
fi

exec "$@"
