#!/usr/bin/env python3
"""生成本地 PM2.5 样例 Parquet 档案。

目录布局（Hive 分区）：
  data/archive/station=<站点>/date=<YYYY-MM-DD>/part-<编号>.parquet

刻意覆盖计划判定的全部分支：
- 全部低于阈值 / 全部高于阈值 / 跨越阈值的文件；
- 含缺测值、全缺测、完全无 null 的文件；
- 关闭统计写入（no_stats）、缺少 pm25 列（missing_column）；
- 截断的损坏文件（unreadable）、非法分区目录（保守保留）；
- 多 row group 文件（统计须跨行组聚合）。

同时生成 data/manifest/manifest.json 供清单模式演示。
用法：python scripts/generate_sample.py [--archive DIR] [--manifest FILE]
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PM25 = "pm25"


def _write(path: Path, values: list, *, write_stats: bool = True,
           extra_columns: dict | None = None, row_group_size: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    arrays = {PM25: pa.array(values, type=pa.float64())}
    if extra_columns:
        arrays.update(extra_columns)
    table = pa.table(arrays)
    pq.write_table(
        table, path,
        write_statistics=write_stats,
        row_group_size=row_group_size or len(values),
        compression="snappy",
    )


def generate(archive: Path, manifest: Path) -> None:
    archive.mkdir(parents=True, exist_ok=True)
    manifest.parent.mkdir(parents=True, exist_ok=True)

    # --- 站点 A：标准三天数据，覆盖超标/达标/跨阈值 ---------------------
    _write(archive / "station=station-A/date=2026-01-01/part-000.parquet",
           [12.0, 18.5, 25.0, 30.1, 9.9])                       # max 30.1，无 null
    _write(archive / "station=station-A/date=2026-01-02/part-000.parquet",
           [42.0, 55.5, 78.0, None, 61.2])                      # 全部偏高，1 个缺测
    _write(archive / "station=station-A/date=2026-01-03/part-000.parquet",
           [20.0, 45.0, 70.0, 33.0, 28.0], row_group_size=2)   # 多 row group 跨阈值

    # --- 站点 B：低浓度 + 全缺测文件 ------------------------------------
    _write(archive / "station=station-B/date=2026-01-01/part-000.parquet",
           [5.0, 8.0, 11.0, 7.5])                               # 全部低于 35
    _write(archive / "station=station-B/date=2026-01-02/part-000.parquet",
           [None, None, None, None])                            # 全缺测
    _write(archive / "station=station-B/date=2026-01-03/part-000.parquet",
           [30.0, 40.0, 50.0])                                  # 跨阈值，无 null

    # --- 站点 C：异常文件三件套 + 非法分区 ------------------------------
    no_stats = archive / "station=station-C/date=2026-01-01/part-nostats.parquet"
    _write(no_stats, [33.0, 66.0, 99.0], write_stats=False)     # no_stats

    missing_col = archive / "station=station-C/date=2026-01-02/part-nocolumn.parquet"
    _write(missing_col, [1.0, 2.0], extra_columns={"other": pa.array([1, 2])})
    # 上面同时写了 pm25 列；重写为只含 other 列的文件以模拟缺列
    pq.write_table(pa.table({"other": pa.array([1.0, 2.0])}), missing_col)

    good = archive / "station=station-C/date=2026-01-03/part-good.parquet"
    _write(good, [88.0, 91.0, 76.5])                            # 超标日
    corrupted = archive / "station=station-C/date=2026-01-03/part-corrupt.parquet"
    _write(corrupted, [50.0, 60.0])
    # 截断 footer 模拟传输损坏：PyArrow 与 DuckDB 都应判失败 -> 保守保留
    raw = corrupted.read_bytes()
    corrupted.write_bytes(raw[: len(raw) // 2])

    # 非法分区目录（缺少 date，分区解析不完整 -> 保守保留）
    _write(archive / "station=station-C/orphan/part-orphan.parquet", [47.0, 52.0])

    # --- 站点 D：边界值，验证闭区间与 min==max --------------------------
    _write(archive / "station=station-D/date=2026-01-01/part-000.parquet", [35.0, 35.0])
    _write(archive / "station=station-D/date=2026-01-02/part-000.parquet", [34.9, 34.9])

    # --- 生成清单（真实替换流程请用 os.replace 原子换入） ---------------
    entries = []
    for path in sorted(archive.rglob("*.parquet")):
        st = path.stat()
        entries.append({
            "path": str(path.relative_to(archive)).replace("\\", "/"),
            "size": st.st_size,
            "mtime_ns": st.st_mtime_ns,
        })
    tmp = manifest.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(
        {"version": 1, "pm25_column": PM25, "files": entries},
        ensure_ascii=False, indent=2,
    ))
    os.replace(tmp, manifest)
    print(f"已生成 {len(entries)} 个 Parquet 文件 -> {archive}")
    print(f"清单已原子写入 -> {manifest}")


def main() -> None:
    ap = argparse.ArgumentParser(description="生成 PM2.5 样例 Parquet 档案")
    default_root = Path(__file__).resolve().parents[1] / "data"
    ap.add_argument("--archive", type=Path, default=default_root / "archive")
    ap.add_argument("--manifest", type=Path, default=default_root / "manifest" / "manifest.json")
    args = ap.parse_args()
    generate(args.archive.resolve(), args.manifest.resolve())


if __name__ == "__main__":
    main()
