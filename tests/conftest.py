"""pytest 共用夹具：构造隔离的临时档案与 FastAPI TestClient。"""
from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pm25_planner.api import create_app  # noqa: E402
from pm25_planner.config import Settings  # noqa: E402


def write_pq(path: Path, values, *, write_stats: bool = True,
             columns: dict | None = None, row_group_size: int | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"pm25": pa.array(values, type=pa.float64())}
    if columns:
        data.update(columns)
    pq.write_table(
        pa.table(data), path, write_statistics=write_stats,
        row_group_size=row_group_size or max(len(values), 1),
    )
    return path


def make_archive(root: Path) -> Path:
    """覆盖全部分支的最小档案。"""
    write_pq(root / "station=A/date=2026-01-01/part-low.parquet", [5.0, 10.0, 20.0])
    write_pq(root / "station=A/date=2026-01-02/part-high.parquet",
             [80.0, 90.0, None])                       # 超标 + 1 null
    write_pq(root / "station=A/date=2026-01-03/part-span.parquet",
             [30.0, 40.0, 50.0], row_group_size=1)    # 多 row group
    write_pq(root / "station=B/date=2026-01-01/part-allnull.parquet",
             [None, None])                             # 全 null
    write_pq(root / "station=B/date=2026-01-02/part-nostats.parquet",
             [33.0, 66.0], write_stats=False)          # no_stats
    # 缺列文件
    nocol = root / "station=B/date=2026-01-03/part-nocol.parquet"
    nocol.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"other": [1, 2]}), nocol)
    # 损坏文件
    corrupt = root / "station=C/date=2026-01-01/part-corrupt.parquet"
    write_pq(corrupt, [50.0])
    raw = corrupt.read_bytes()
    corrupt.write_bytes(raw[: len(raw) // 2])
    # 非法分区（缺 date）
    write_pq(root / "station=C/orphan/part.parquet", [47.0])
    # 边界值文件
    write_pq(root / "station=D/date=2026-01-01/part-eq.parquet", [35.0, 35.0])
    return root


@pytest.fixture()
def archive(tmp_path: Path) -> Path:
    return make_archive(tmp_path / "archive")


@pytest.fixture()
def settings(archive: Path, tmp_path: Path) -> Settings:
    return Settings(
        archive_dir=archive, manifest_path=None, pm25_column="pm25",
        preview_max_files=50, meta_cache_size=128,
    )


@pytest.fixture()
def client(settings: Settings) -> TestClient:
    return TestClient(create_app(settings))
