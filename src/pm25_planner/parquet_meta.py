"""Parquet 元数据提取与交叉核对。

只读取 footer / 行组统计，不扫描数据行：
- 主路径：PyArrow ``ParquetFile`` 聚合各 row group 的 min/max/null_count；
- 核对路径：DuckDB ``read_parquet`` 独立重算同一组指标，供测试与调试接口比对。

任何无法确认的情况（文件缺失、footer 损坏、列缺失、无统计）都返回
显式 ``stats_status``，由计划层执行“保守保留”，绝不静默排除。
"""
from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pyarrow.lib as _pa_lib
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

StatsStatus = Literal["ok", "no_stats", "unreadable", "missing_column"]


@dataclass(frozen=True)
class FileMeta:
    """单个 Parquet 文件的分区值与 PM2.5 列统计快照。"""

    path: Path
    rel_path: str
    station: str | None
    date: str | None
    size: int
    mtime_ns: int
    exists: bool
    pm25_min: float | None
    pm25_max: float | None
    null_count: int | None
    row_count: int | None
    stats_status: StatsStatus
    error: str | None


def parse_partitions(rel_path: str) -> tuple[str | None, str | None]:
    """从相对路径解析 ``station`` / ``date`` 分区值。

    支持 Hive 风格（``station=A/date=2026-01-01/part.parquet``），
    也兼容两级裸目录（``A/2026-01-01/part.parquet``，第二级须为合法 ISO 日期）。
    无法确认的值返回 ``None``，交由计划层保守处理。
    """
    parts = [p for p in rel_path.replace("\\", "/").split("/") if p and p != "."]
    station: str | None = None
    date_value: str | None = None

    for part in parts[:-1]:  # 最后一段是文件名
        if "=" in part:
            key, _, raw = part.partition("=")
            key, raw = key.strip(), raw.strip()
            if key == "station":
                station = raw or None
            elif key == "date":
                if _is_iso_date(raw):
                    date_value = raw
                # 非法日期保持 None，而不是猜测
        elif station is None and date_value is None and len(parts) >= 3:
            # 裸目录降级解析：仅当两级目录都像分区值时采用
            if parts.index(part) == 0:
                station = part

    if date_value is None and len(parts) >= 3:
        maybe_date = parts[-2]
        if "=" not in maybe_date and _is_iso_date(maybe_date):
            date_value = maybe_date

    return station, date_value


def _is_iso_date(raw: str) -> bool:
    try:
        _dt.date.fromisoformat(raw)
    except (ValueError, TypeError):
        return False
    return True


def extract_file_meta(path: Path, rel_path: str, pm25_column: str) -> FileMeta:
    """提取单文件元数据；任何异常都转成 ``unreadable`` 而非抛出。"""
    station, date_value = parse_partitions(rel_path)
    try:
        st = path.stat()
        size, mtime_ns = st.st_size, st.st_mtime_ns
    except OSError as exc:
        return FileMeta(
            path=path, rel_path=rel_path, station=station, date=date_value,
            size=0, mtime_ns=0, exists=False,
            pm25_min=None, pm25_max=None, null_count=None, row_count=None,
            stats_status="unreadable", error=f"文件无法访问: {exc.__class__.__name__}: {exc}",
        )

    try:
        pf = pq.ParquetFile(path)
        row_count: int | None = pf.metadata.num_rows if pf.metadata is not None else None
        names = pf.schema_arrow.names
        if pm25_column not in names:
            return FileMeta(
                path=path, rel_path=rel_path, station=station, date=date_value,
                size=size, mtime_ns=mtime_ns, exists=True,
                pm25_min=None, pm25_max=None, null_count=None, row_count=row_count,
                stats_status="missing_column",
                error=f"Schema 中不存在列 {pm25_column!r}，实际列: {names}",
            )

        col_idx = names.index(pm25_column)
        mins: list[float] = []
        maxs: list[float] = []
        null_total = 0
        any_stats = False
        for rg_idx in range(pf.num_row_groups):
            rg = pf.metadata.row_group(rg_idx)
            chunk = rg.column(col_idx)
            stats = chunk.statistics
            if stats is None or not chunk.is_stats_set:
                continue  # 该 row group 无统计：跳过聚合，最终降级 no_stats
            any_stats = True
            if stats.has_null_count:
                null_total += int(stats.null_count or 0)
            if stats.has_min_max:
                # 物理类型可能是 int32/int64/float/double，统一转 float
                mins.append(float(stats.min))
                maxs.append(float(stats.max))

        if not any_stats:
            return FileMeta(
                path=path, rel_path=rel_path, station=station, date=date_value,
                size=size, mtime_ns=mtime_ns, exists=True,
                pm25_min=None, pm25_max=None, null_count=None,
                row_count=row_count,
                stats_status="no_stats",
                error="所有 row group 均未写入 min/max/null_count 统计",
            )

        # any_stats=True 但无 min/max：典型为“全 null 列”，
        # null_count 仍可信，按 ok 返回（min/max 为 None 表示无任何非空值）
        return FileMeta(
            path=path, rel_path=rel_path, station=station, date=date_value,
            size=size, mtime_ns=mtime_ns, exists=True,
            pm25_min=min(mins) if mins else None,
            pm25_max=max(maxs) if maxs else None,
            null_count=null_total, row_count=row_count,
            stats_status="ok", error=None,
        )
    except (OSError, _pa_lib.ArrowInvalid, _pa_lib.ArrowIOError, ValueError, EOFError) as exc:
        logger.warning("Parquet 元数据读取失败 %s: %s", rel_path, exc)
        return FileMeta(
            path=path, rel_path=rel_path, station=station, date=date_value,
            size=size, mtime_ns=mtime_ns, exists=True,
            pm25_min=None, pm25_max=None, null_count=None, row_count=None,
            stats_status="unreadable", error=f"{exc.__class__.__name__}: {exc}",
        )


def duckdb_cross_check(path: Path, pm25_column: str) -> dict:
    """用 DuckDB 独立重算 row_count / null_count / min / max，核对 PyArrow 结果。

    仅用于测试与 ``/api/debug/duckdb-stats`` 调试接口；计划生成本身不依赖它。
    """
    import duckdb

    quoted = str(path).replace("'", "''")
    col = pm25_column.replace('"', '""')
    sql = (
        f"SELECT count(*) AS row_count, "
        f"count(*) FILTER (WHERE \"{col}\" IS NULL) AS null_count, "
        f"min(\"{col}\") AS col_min, max(\"{col}\") AS col_max "
        f"FROM read_parquet('{quoted}')"
    )
    con = duckdb.connect()
    try:
        row = con.execute(sql).fetchone()
        return {
            "ok": True,
            "row_count": int(row[0] or 0),
            "null_count": int(row[1] or 0),
            "pm25_min": None if row[2] is None else float(row[2]),
            "pm25_max": None if row[3] is None else float(row[3]),
        }
    except Exception as exc:  # DuckDB 对损坏文件会抛异常：如实返回
        return {"ok": False, "error": f"{exc.__class__.__name__}: {exc}"}
    finally:
        con.close()
