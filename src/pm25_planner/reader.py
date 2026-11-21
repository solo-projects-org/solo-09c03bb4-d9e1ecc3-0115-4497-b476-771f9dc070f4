"""计划执行预览：用 DuckDB 实际读取“保留文件”，验证计划可执行。

读取同样绑定快照 ID：快照已变化则拒绝执行，避免拿着过期计划读新数据。
仅用于小批量预览（受 ``preview_max_files`` 限制），不承担批量分析职责。
"""
from __future__ import annotations

import logging

import duckdb

from .config import Settings
from .models import PreviewResponse, PreviewRow
from .planner import build_plan
from .snapshot import Snapshot

logger = logging.getLogger(__name__)


def execute_preview(snapshot: Snapshot, current_id: str, request,
                    settings: Settings) -> PreviewResponse:
    plan = build_plan(snapshot, request)
    valid = snapshot.snapshot_id == current_id
    notes: list[str] = list(plan.notes)

    if not valid:
        return PreviewResponse(
            snapshot_id=snapshot.snapshot_id, valid_snapshot=False,
            row_count=0, files_read=0, rows=[],
            notes=notes + ["快照已变化，拒绝按旧计划读取，请重新生成计划"],
        )

    kept = [d for d in plan.files if d.decision == "KEEP"]
    if len(kept) > settings.preview_max_files:
        return PreviewResponse(
            snapshot_id=snapshot.snapshot_id, valid_snapshot=True,
            row_count=0, files_read=0, rows=[],
            notes=notes + [
                f"保留文件 {len(kept)} 个超过预览上限 {settings.preview_max_files}，"
                "拒绝读取以避免计划外大查询（计划本身仍然有效）"
            ],
        )

    if not kept:
        return PreviewResponse(
            snapshot_id=snapshot.snapshot_id, valid_snapshot=True,
            row_count=0, files_read=0, rows=[],
            notes=notes + ["无保留文件，无需读取"],
        )

    col = settings.pm25_column.replace('"', '""')
    con = duckdb.connect()
    rows: list[PreviewRow] = []
    files_read = 0
    errors: list[str] = []
    try:
        for d in kept:
            full = snapshot.archive_dir / d.path
            quoted = str(full).replace("'", "''")
            try:
                fetched = con.execute(
                    f"SELECT \"{col}\" FROM read_parquet('{quoted}')"
                ).fetchall()
            except Exception as exc:  # 损坏文件：如实报告，与保守保留策略呼应
                errors.append(f"{d.path}: {exc.__class__.__name__}: {exc}")
                continue
            files_read += 1
            for (value,) in fetched:
                rows.append(PreviewRow(
                    station=d.station, date=d.date,
                    source_file=d.path, pm25=None if value is None else float(value),
                ))
    finally:
        con.close()

    if errors:
        notes.append("部分保守保留的文件实际读取失败（符合预期，未漏判可读文件）："
                     + "；".join(errors))
    return PreviewResponse(
        snapshot_id=snapshot.snapshot_id, valid_snapshot=True,
        row_count=len(rows), files_read=files_read, rows=rows, notes=notes,
    )
