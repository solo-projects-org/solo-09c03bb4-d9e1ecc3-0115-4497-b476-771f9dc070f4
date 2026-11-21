#!/usr/bin/env python3
"""演示“清单原子替换 -> 旧计划快照失效”的完整流程。

步骤：
1. 读取当前清单，快照 ID 记为 old_id；
2. 向档案新增一个分区文件；
3. 在临时文件写好新清单后用 os.replace 原子替换（替换期间读不到半截 JSON）；
4. 重新加载得到 new_id；调用 /api/plan/verify 即会判定 old_id 失效。

用法：python scripts/replace_manifest_demo.py [--archive DIR] [--manifest FILE]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pm25_planner.config import Settings  # noqa: E402
from pm25_planner.snapshot import SnapshotStore  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="清单替换与快照失效演示")
    default_root = Path(__file__).resolve().parents[1] / "data"
    ap.add_argument("--archive", type=Path, default=default_root / "archive")
    ap.add_argument("--manifest", type=Path,
                    default=default_root / "manifest" / "manifest.json")
    args = ap.parse_args()

    archive = args.archive.resolve()
    manifest = args.manifest.resolve()
    settings = Settings.from_env()
    object.__setattr__(settings, "archive_dir", archive)
    object.__setattr__(settings, "manifest_path", manifest)
    store = SnapshotStore(settings)

    old = store.current()
    print(f"[1] 替换前快照: {old.snapshot_id} ({len(old.files)} 个文件)")

    new_file = archive / "station=station-A/date=2026-01-04/part-new.parquet"
    new_file.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"pm25": pa.array([48.0, 52.0], type=pa.float64())}), new_file)

    payload = json.loads(manifest.read_text())
    st = new_file.stat()
    payload["files"].append({
        "path": str(new_file.relative_to(archive)).replace("\\", "/"),
        "size": st.st_size, "mtime_ns": st.st_mtime_ns,
    })
    tmp = manifest.with_name("manifest.json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    os.replace(tmp, manifest)  # 原子替换：读者只会看到旧版或新版
    print("[2] 已新增文件并原子替换清单")

    store.invalidate()
    new = store.current()
    print(f"[3] 替换后快照: {new.snapshot_id} ({len(new.files)} 个文件)")
    print(f"[4] 旧快照 {old.snapshot_id} 是否仍有效: "
          f"{old.snapshot_id == new.snapshot_id}（False 即旧计划已失效）")


if __name__ == "__main__":
    main()
