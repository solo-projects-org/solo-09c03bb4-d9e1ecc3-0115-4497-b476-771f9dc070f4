"""读取计划生成：基于分区值与 min/max/null_count 的裁剪判定。

判定原则（与需求逐条对应）：
1. 分区裁剪：站点/日期不匹配可安全排除，原因逐文件写明；
2. 统计裁剪：仅当列统计完整（stats_status=ok）且 min/max 与请求区间
   完全不相交时才排除；
3. 缺测策略：include_nulls=False 且文件仅可能贡献 null 行时排除；
4. 保守保留：文件缺失、footer 损坏、列缺失、无统计、分区值无法解析
   等任何“无法确认”的情况一律 KEEP，并显式标注原因，绝不静默排除。

原因码（reason_codes）稳定，供测试与自动化核对；reasons 为中文说明。
"""
from __future__ import annotations

from datetime import date
from typing import Any

from .models import FileDecision, PlanRequest, PlanResponse
from .parquet_meta import FileMeta
from .snapshot import Snapshot

# 原因码常量
PARTITION_STATION_EXCLUDED = "PARTITION_STATION_EXCLUDED"
PARTITION_DATE_EXCLUDED = "PARTITION_DATE_EXCLUDED"
PARTITION_UNPARSEABLE = "PARTITION_UNPARSEABLE_KEEP"
RANGE_DISJOINT = "STATS_RANGE_DISJOINT"
NO_NULLS_SKIPPED = "NULLS_PRESENT_BUT_EXCLUDED"
UNREADABLE_KEEP = "FILE_UNREADABLE_KEEP"
MISSING_COLUMN_KEEP = "MISSING_COLUMN_KEEP"
NO_STATS_KEEP = "NO_STATS_KEEP"
NOT_EXISTS_KEEP = "FILE_MISSING_KEEP"
STATS_MATCH_KEEP = "STATS_MATCH_KEEP"
NULLS_INCLUDED_KEEP = "NULLS_INCLUDED_KEEP"


def _intersects(lo: float, hi: float, q_lo: float | None, q_hi: float | None) -> bool:
    if q_lo is not None and hi < q_lo:
        return False
    if q_hi is not None and lo > q_hi:
        return False
    return True


def decide_file(meta: FileMeta, req: PlanRequest) -> FileDecision:
    """对单个文件执行完整判定链，返回保留/排除结论与全部原因。"""
    reasons: list[str] = []
    codes: list[str] = []
    keep = True

    # ---- 0. 文件不存在（清单引用了已消失的文件）：保守保留 --------------
    if not meta.exists:
        keep = True
        reasons.append("文件在档案目录中不存在，无法确认内容，保守保留")
        codes.append(NOT_EXISTS_KEEP)
        return _build(meta, keep, reasons, codes)

    # ---- 1. 分区值完整性：任一分区值缺失即不能安全做分区裁剪 ------------
    partition_safe = meta.station is not None and meta.date is not None
    if not partition_safe:
        reasons.append("分区路径无法解析出完整的 station/date，跳过分区裁剪，保守保留")
        codes.append(PARTITION_UNPARSEABLE)

    # ---- 2. 站点裁剪（仅在分区可安全解析时执行） ------------------------
    if partition_safe and req.stations and meta.station not in req.stations:
        reasons.append(f"站点 {meta.station!r} 不在选择范围 {sorted(req.stations)}，排除")
        codes.append(PARTITION_STATION_EXCLUDED)
        return _build(meta, False, reasons, codes)

    # ---- 3. 日期裁剪（仅在分区可安全解析时执行） ------------------------
    if partition_safe:
        try:
            part_date = date.fromisoformat(meta.date)  # type: ignore[arg-type]
            if req.date_from and part_date < req.date_from:
                reasons.append(f"日期 {meta.date} 早于起始 {req.date_from}，排除")
                codes.append(PARTITION_DATE_EXCLUDED)
                return _build(meta, False, reasons, codes)
            if req.date_to and part_date > req.date_to:
                reasons.append(f"日期 {meta.date} 晚于截止 {req.date_to}，排除")
                codes.append(PARTITION_DATE_EXCLUDED)
                return _build(meta, False, reasons, codes)
        except ValueError:
            reasons.append(f"日期分区值 {meta.date!r} 非法，跳过日期裁剪，保守保留")
            codes.append(PARTITION_UNPARSEABLE)

    # ---- 4. 依据统计状态判定 --------------------------------------------
    if meta.stats_status == "unreadable":
        reasons.append(f"Parquet footer 损坏或不可读（{meta.error}），保守保留")
        codes.append(UNREADABLE_KEEP)
        return _build(meta, True, reasons, codes)

    if meta.stats_status == "missing_column":
        reasons.append(f"缺少 PM2.5 列（{meta.error}），无法判断，保守保留")
        codes.append(MISSING_COLUMN_KEEP)
        return _build(meta, True, reasons, codes)

    if meta.stats_status == "no_stats":
        reasons.append("文件可用但缺少 min/max 统计，无法证明区间不相交，保守保留")
        codes.append(NO_STATS_KEEP)
        return _build(meta, True, reasons, codes)

    # stats_status == ok
    range_requested = req.pm25_min is not None or req.pm25_max is not None
    # min/max 同时为 None：该文件没有任何非空值（全 null 列），统计仍可信
    all_null_column = meta.pm25_min is None and meta.pm25_max is None
    has_possible_value = False if all_null_column else True
    if range_requested and not all_null_column:
        has_possible_value = _intersects(
            meta.pm25_min, meta.pm25_max, req.pm25_min, req.pm25_max  # type: ignore[arg-type]
        )
        if has_possible_value:
            reasons.append(
                f"统计区间 [{meta.pm25_min}, {meta.pm25_max}] 与请求区间"
                f" [{req.pm25_min if req.pm25_min is not None else '-∞'},"
                f" {req.pm25_max if req.pm25_max is not None else '+∞'}] 相交，保留"
            )
            codes.append(STATS_MATCH_KEEP)
        else:
            reasons.append(
                f"统计区间 [{meta.pm25_min}, {meta.pm25_max}] 与请求区间不相交，排除"
            )
            codes.append(RANGE_DISJOINT)
    elif not range_requested and not all_null_column:
        reasons.append("未设置 PM2.5 区间，文件通过分区裁剪，保留")
        codes.append(STATS_MATCH_KEEP)

    has_nulls = bool(meta.null_count)
    if all_null_column:
        reasons.append(f"文件 {meta.row_count} 行全部为缺测值（null_count={meta.null_count}）")
    if has_nulls and req.include_nulls:
        reasons.append(f"文件含 {meta.null_count} 个缺测值，按“包含缺测”保留")
        codes.append(NULLS_INCLUDED_KEEP)
        return _build(meta, True, reasons, codes)

    if not req.include_nulls and has_nulls and not has_possible_value:
        if not partition_safe:
            reasons.append("分区值不完整无法确认归属，即使按“不包含缺测”本可排除，仍保守保留")
            return _build(meta, True, reasons, codes)
        reasons.append(
            f"文件含 {meta.null_count} 个缺测值且无落在区间内的非空值，"
            "按“不包含缺测”排除"
        )
        codes.append(NO_NULLS_SKIPPED)
        return _build(meta, False, reasons, codes)

    if not partition_safe and not has_possible_value:
        # 统计虽不相交，但分区归属无法确认：不做排除，强制保守保留
        reasons.append("尽管统计区间与请求不相交，因分区值不完整无法确认归属，仍保守保留")
        return _build(meta, True, reasons, codes)

    return _build(meta, has_possible_value, reasons, codes)


def _build(meta: FileMeta, keep: bool, reasons: list[str], codes: list[str]) -> FileDecision:
    return FileDecision(
        path=meta.rel_path,
        station=meta.station,
        date=meta.date,
        size_bytes=meta.size,
        decision="KEEP" if keep else "EXCLUDE",
        reasons=reasons,
        reason_codes=codes,
        pm25_min=meta.pm25_min,
        pm25_max=meta.pm25_max,
        null_count=meta.null_count,
        row_count=meta.row_count,
        stats_status=meta.stats_status,
        exists=meta.exists,
    )


def build_plan(snapshot: Snapshot, req: PlanRequest) -> PlanResponse:
    """对快照内全部文件执行判定，汇总计划字节数与可选分区值。"""
    decisions = [decide_file(m, req) for m in sorted(snapshot.files, key=lambda m: m.rel_path)]
    kept = [d.path for d in decisions if d.decision == "KEEP"]
    planned_bytes = sum(d.size_bytes for d in decisions if d.decision == "KEEP")

    stations = sorted({m.station for m in snapshot.files if m.station})
    dates = sorted({m.date for m in snapshot.files if m.date})

    notes: list[str] = []
    if any(d.stats_status in ("unreadable", "no_stats", "missing_column") or
           not d.exists for d in decisions if d.decision == "KEEP"):
        notes.append(
            "存在统计缺失/损坏或不可访问的文件，已按保守策略保留，"
            "实际读取时可能不包含匹配记录（误保留，不会漏读）"
        )
    if any(PARTITION_UNPARSEABLE in d.reason_codes for d in decisions):
        notes.append("存在分区路径无法解析的文件，已保守保留")
    if not kept:
        notes.append("当前条件下没有必须读取的文件（planned_bytes=0）")

    return PlanResponse(
        snapshot_id=snapshot.snapshot_id,
        snapshot_source=snapshot.source,  # type: ignore[arg-type]
        generated_at=snapshot.generated_at,
        request=req,
        files=decisions,
        kept_files=kept,
        kept_count=len(kept),
        excluded_count=len(decisions) - len(kept),
        total_files=len(decisions),
        planned_bytes=planned_bytes,
        available_stations=stations,
        available_dates=dates,
        notes=notes,
    )


def plan_request_from_dict(payload: dict[str, Any]) -> PlanRequest:
    """供测试/脚本使用的宽松构造入口。"""
    return PlanRequest(**payload)
