"""计划判定核心逻辑测试。"""
from __future__ import annotations

from datetime import date

from pm25_planner.config import Settings
from pm25_planner.models import PlanRequest
from pm25_planner.planner import (
    PARTITION_DATE_EXCLUDED,
    MISSING_COLUMN_KEEP,
    NO_NULLS_SKIPPED,
    NO_STATS_KEEP,
    NOT_EXISTS_KEEP,
    NULLS_INCLUDED_KEEP,
    PARTITION_STATION_EXCLUDED,
    PARTITION_UNPARSEABLE,
    RANGE_DISJOINT,
    STATS_MATCH_KEEP,
    UNREADABLE_KEEP,
    build_plan,
)
from pm25_planner.snapshot import SnapshotStore


def _store(settings) -> SnapshotStore:
    return SnapshotStore(settings)


def _codes(plan, path_suffix):
    for f in plan.files:
        if f.path.endswith(path_suffix):
            return f
    raise KeyError(path_suffix)


def test_partition_pruning_station_and_date(settings):
    snap = _store(settings).current()
    req = PlanRequest(stations=["A"], date_from=date(2026, 1, 2),
                      date_to=date(2026, 1, 2))
    plan = build_plan(snap, req)
    low = _codes(plan, "part-low.parquet")
    assert low.decision == "EXCLUDE"
    assert PARTITION_DATE_EXCLUDED in low.reason_codes
    high = _codes(plan, "part-high.parquet")
    assert high.decision == "KEEP"
    # 分区完整的 B/D 文件全部因站点被排除
    assert all(f.decision == "EXCLUDE" for f in plan.files
               if f.station and f.station != "A" and f.date is not None)


def test_station_exclusion_reason(settings):
    snap = _store(settings).current()
    plan = build_plan(snap, PlanRequest(stations=["B"]))
    a_file = _codes(plan, "part-low.parquet")
    assert a_file.decision == "EXCLUDE"
    assert PARTITION_STATION_EXCLUDED in a_file.reason_codes


def test_range_disjoint_excludes_only_when_stats_complete(settings):
    snap = _store(settings).current()
    # 区间 [85, 200]：low 文件统计 [5,20] 不相交 -> 排除
    plan = build_plan(snap, PlanRequest(pm25_min=85, pm25_max=200))
    low = _codes(plan, "part-low.parquet")
    assert low.decision == "EXCLUDE"
    assert RANGE_DISJOINT in low.reason_codes
    # high 文件 [80,90] 与 [85,200] 相交 -> 保留
    high = _codes(plan, "part-high.parquet")
    assert high.decision == "KEEP"
    assert STATS_MATCH_KEEP in high.reason_codes


def test_closed_interval_boundary_inclusive(settings):
    snap = _store(settings).current()
    # min==max==35，查询下界 35 必须相交（闭区间）
    plan = build_plan(snap, PlanRequest(pm25_min=35))
    eq = _codes(plan, "part-eq.parquet")
    assert eq.decision == "KEEP"
    # 查询上界 34.9 则不相交
    plan2 = build_plan(snap, PlanRequest(pm25_max=34.9))
    eq2 = _codes(plan2, "part-eq.parquet")
    assert eq2.decision == "EXCLUDE"
    assert RANGE_DISJOINT in eq2.reason_codes


def test_null_handling_include_and_exclude(settings):
    snap = _store(settings).current()
    # 全 null 文件：包含缺测 -> 保留；不包含 -> 排除
    plan_inc = build_plan(snap, PlanRequest(include_nulls=True))
    allnull = _codes(plan_inc, "part-allnull.parquet")
    assert allnull.decision == "KEEP"
    assert NULLS_INCLUDED_KEEP in allnull.reason_codes
    assert allnull.null_count == 2

    plan_exc = build_plan(snap, PlanRequest(
        include_nulls=False, pm25_min=80, pm25_max=200))
    allnull2 = _codes(plan_exc, "part-allnull.parquet")
    assert allnull2.decision == "EXCLUDE"
    assert NO_NULLS_SKIPPED in allnull2.reason_codes

    # 高值文件含 1 null 但也含区间内值：即使不包含缺测仍保留
    high = _codes(plan_exc, "part-high.parquet")
    assert high.decision == "KEEP"


def test_no_stats_conservative_keep(settings):
    snap = _store(settings).current()
    plan = build_plan(snap, PlanRequest(pm25_min=1000))
    f = _codes(plan, "part-nostats.parquet")
    assert f.decision == "KEEP"
    assert NO_STATS_KEEP in f.reason_codes


def test_missing_column_conservative_keep(settings):
    snap = _store(settings).current()
    plan = build_plan(snap, PlanRequest(pm25_min=1000))
    f = _codes(plan, "part-nocol.parquet")
    assert f.decision == "KEEP"
    assert MISSING_COLUMN_KEEP in f.reason_codes
    assert f.stats_status == "missing_column"


def test_corrupted_file_conservative_keep(settings):
    snap = _store(settings).current()
    plan = build_plan(snap, PlanRequest(pm25_min=1000))
    f = _codes(plan, "part-corrupt.parquet")
    assert f.decision == "KEEP"
    assert UNREADABLE_KEEP in f.reason_codes
    assert f.stats_status == "unreadable"


def test_unparseable_partition_conservative_keep(settings):
    snap = _store(settings).current()
    # 即使站点白名单不含 C、日期窗口不覆盖，孤儿文件仍须保留
    plan = build_plan(snap, PlanRequest(
        stations=["A"], date_from=date(2030, 1, 1), date_to=date(2030, 12, 31)))
    f = _codes(plan, "part.parquet")
    assert f.decision == "KEEP"
    assert PARTITION_UNPARSEABLE in f.reason_codes


def test_unparseable_partition_forces_keep_even_when_stats_disjoint(settings):
    """孤儿文件统计与区间不相交、且不包含缺测时，仍因分区不完整强制保留。"""
    snap = _store(settings).current()
    plan = build_plan(snap, PlanRequest(
        pm25_min=1000, include_nulls=False,
        stations=["A"], date_from=date(2030, 1, 1), date_to=date(2030, 12, 31)))
    f = _codes(plan, "part.parquet")
    assert f.decision == "KEEP"
    assert PARTITION_UNPARSEABLE in f.reason_codes


def test_planned_bytes_sum_and_counts(settings):
    snap = _store(settings).current()
    plan_all = build_plan(snap, PlanRequest())
    assert plan_all.kept_count + plan_all.excluded_count == plan_all.total_files
    # 无条件查询：除无匹配分区外全部保留；字节数等于保留文件之和
    assert plan_all.planned_bytes == sum(f.size_bytes for f in plan_all.files
                                         if f.decision == "KEEP")
    plan_narrow = build_plan(snap, PlanRequest(pm25_min=1000, include_nulls=False))
    # 强窄区间 + 不要 null：统计完整且不相交的文件被排除，异常文件仍保留
    assert plan_narrow.kept_count < plan_narrow.total_files
    kept_suffixes = {f.path.split("/")[-1] for f in plan_narrow.files
                     if f.decision == "KEEP"}
    assert "part-nostats.parquet" in kept_suffixes
    assert "part-corrupt.parquet" in kept_suffixes
    assert "part-nocol.parquet" in kept_suffixes


def test_missing_manifest_entry_is_kept(settings, tmp_path):
    # 清单引用一个磁盘上不存在的文件 -> NOT EXISTS 保守保留
    import json
    manifest = tmp_path / "manifest.json"
    ghost = "station=Z/date=2026-01-01/ghost.parquet"
    manifest.write_text(json.dumps({"files": [
        {"path": ghost, "size": 123, "mtime_ns": 1}
    ]}))
    settings2 = Settings(
        archive_dir=settings.archive_dir, manifest_path=manifest,
        pm25_column="pm25", preview_max_files=50, meta_cache_size=128,
    )
    snap = SnapshotStore(settings2).current()
    plan = build_plan(snap, PlanRequest(stations=["A"]))
    f = _codes(plan, "ghost.parquet")
    assert f.decision == "KEEP"
    assert NOT_EXISTS_KEEP in f.reason_codes
    assert f.exists is False


def test_multi_rowgroup_stats_aggregation(settings):
    snap = _store(settings).current()
    f = _codes(build_plan(snap, PlanRequest()), "part-span.parquet")
    # 三个 row group 各自单行 [30]/[40]/[50]，聚合后 min=30 max=50
    assert f.pm25_min == 30.0 and f.pm25_max == 50.0
    assert f.row_count == 3


def test_invalid_requests(settings):
    import pydantic
    # min > max 应校验失败
    try:
        PlanRequest(pm25_min=100, pm25_max=10)
    except pydantic.ValidationError:
        pass
    else:
        raise AssertionError("应当拒绝 pm25_min > pm25_max")
