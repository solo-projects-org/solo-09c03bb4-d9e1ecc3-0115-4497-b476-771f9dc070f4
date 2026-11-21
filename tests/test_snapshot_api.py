"""快照绑定、清单原子替换失效与 API 行为测试。"""
from __future__ import annotations

import json
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq

from pm25_planner.config import Settings
from pm25_planner.parquet_meta import duckdb_cross_check, extract_file_meta
from pm25_planner.snapshot import SnapshotStore


def test_snapshot_stable_without_change(settings):
    store = SnapshotStore(settings)
    s1 = store.current()
    s2 = store.current()
    assert s1.snapshot_id == s2.snapshot_id


def test_snapshot_changes_when_file_added(settings, archive):
    store = SnapshotStore(settings)
    old_id = store.current().snapshot_id
    new_file = archive / "station=A/date=2026-01-04/new.parquet"
    new_file.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"pm25": pa.array([40.0])}), new_file)
    # mtime 纳秒可能与目录扫描同刻，强制刷新缓存指纹
    store.invalidate()
    assert store.current().snapshot_id != old_id


def test_manifest_atomic_replacement_invalidates_old_plan(settings, archive, tmp_path):
    manifest = tmp_path / "manifest.json"

    def write_manifest():
        entries = []
        for p in sorted(archive.rglob("*.parquet")):
            st = p.stat()
            entries.append({"path": str(p.relative_to(archive)),
                            "size": st.st_size, "mtime_ns": st.st_mtime_ns})
        tmp = manifest.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"files": entries}))
        os.replace(tmp, manifest)

    write_manifest()
    ms = Settings(archive_dir=archive, manifest_path=manifest, pm25_column="pm25",
                  preview_max_files=50, meta_cache_size=128)
    store = SnapshotStore(ms)
    old_id = store.current().snapshot_id
    assert store.current().source == "manifest"

    # 原子替换：新增一个文件后整体换清单
    extra = archive / "station=A/date=2026-01-05/extra.parquet"
    extra.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"pm25": pa.array([60.0])}), extra)
    time.sleep(0.01)
    write_manifest()
    store.invalidate()
    new_snap = store.current()
    assert new_snap.snapshot_id != old_id

    # 旧 ID 经 verify 判定失效
    from fastapi.testclient import TestClient
    from pm25_planner.api import create_app
    client = TestClient(create_app(ms))
    r = client.post("/api/plan/verify", json={"snapshot_id": old_id})
    assert r.status_code == 200
    body = r.json()
    assert body["valid"] is False
    assert body["current_snapshot_id"] == new_snap.snapshot_id


def test_plan_endpoint_rejects_stale_expected_snapshot(client, settings):
    r = client.post("/api/plan", json={})
    assert r.status_code == 200
    real_id = r.json()["snapshot_id"]
    r2 = client.post("/api/plan", json={"expected_snapshot_id": "deadbeefdeadbeef"})
    assert r2.status_code == 409
    detail = r2.json()["detail"]
    assert detail["error"] == "STALE_SNAPSHOT"
    assert detail["current_snapshot_id"] == real_id


def test_verify_endpoint(client):
    snap = client.get("/api/snapshot").json()
    ok = client.post("/api/plan/verify", json={"snapshot_id": snap["snapshot_id"]})
    assert ok.json()["valid"] is True
    bad = client.post("/api/plan/verify", json={"snapshot_id": "0000000000000000"})
    assert bad.json()["valid"] is False


def test_missing_manifest_returns_503(archive, tmp_path):
    from fastapi.testclient import TestClient
    from pm25_planner.api import create_app
    ms = Settings(archive_dir=archive, manifest_path=tmp_path / "nope.json",
                  pm25_column="pm25", preview_max_files=50, meta_cache_size=128)
    client = TestClient(create_app(ms))
    assert client.get("/api/snapshot").status_code == 503


def test_preview_execution_and_snapshot_binding(client, settings):
    # 不限制站点：C 站损坏文件会被保守保留，实读时失败并写入 notes
    body = {"pm25_min": 80, "include_nulls": True}
    r = client.post("/api/plan/preview", json=body)
    assert r.status_code == 200
    data = r.json()
    assert data["valid_snapshot"] is True
    assert data["files_read"] >= 1
    # high 文件含 null，预览行里应出现 null
    assert any(row["pm25"] is None for row in data["rows"])
    # 损坏文件被保守保留但读取失败 -> notes 中应如实说明
    assert any("读取失败" in n for n in data["notes"])

    # 绑定伪造旧快照 -> 拒绝执行
    r2 = client.post("/api/plan/preview",
                     json={**body, "expected_snapshot_id": "ffffffffffffffff"})
    assert r2.json()["valid_snapshot"] is False


def test_pyarrow_duckdb_stats_agree(settings, archive):
    """两个引擎对同一批健康文件的统计必须一致。"""
    store = SnapshotStore(settings)
    snap = store.current()
    checked = 0
    for meta in snap.files:
        if meta.stats_status != "ok":
            continue
        cross = duckdb_cross_check(meta.path, "pm25")
        assert cross["ok"], cross
        assert cross["row_count"] == meta.row_count
        assert cross["null_count"] == meta.null_count
        if cross["pm25_min"] is None:
            # 全 null 列：两引擎都应给出 None min/max
            assert meta.pm25_min is None and meta.pm25_max is None
        else:
            assert abs(cross["pm25_min"] - meta.pm25_min) < 1e-9
            assert abs(cross["pm25_max"] - meta.pm25_max) < 1e-9
        checked += 1
    assert checked >= 4  # 确保确实核对了一批文件


def test_duckdb_rejects_corrupted_file(settings, archive):
    corrupt = archive / "station=C/date=2026-01-01/part-corrupt.parquet"
    cross = duckdb_cross_check(corrupt, "pm25")
    assert cross["ok"] is False and "error" in cross
    meta = extract_file_meta(corrupt,
                             "station=C/date=2026-01-01/part-corrupt.parquet", "pm25")
    assert meta.stats_status == "unreadable"


def test_debug_endpoint_and_path_traversal_block(client):
    r = client.get("/api/debug/duckdb-stats",
                   params={"path": "station=A/date=2026-01-01/part-low.parquet"})
    assert r.status_code == 200
    payload = r.json()
    assert payload["duckdb"]["ok"] is True
    assert payload["pyarrow"]["stats_status"] == "ok"

    bad = client.get("/api/debug/duckdb-stats", params={"path": "../../etc/passwd"})
    assert bad.status_code == 400


def test_health_and_index(client):
    h = client.get("/api/health").json()
    assert h["status"] == "ok" and h["file_count"] > 0
    assert client.get("/").status_code == 200
    assert client.get("/static/index.html").status_code == 200
