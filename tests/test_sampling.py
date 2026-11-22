"""月报抽查名单：抽样分布、增量等价、失败保留与重启恢复测试。"""
from __future__ import annotations

import json
import os
import time
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from pm25_planner.api import create_app
from pm25_planner.config import Settings
from pm25_planner.sampling import (
    EXCEED_THRESHOLD,
    KIND_EXCEED,
    KIND_MISSING,
    SampleError,
    SampleStore,
    build_full,
    build_spec,
    classify,
    compare_with_full,
    draw_key,
    month_bounds,
    scan_file,
)
from pm25_planner.snapshot import ManifestError, SnapshotStore


# --------------------------------------------------------------------------- #
# 夹具
# --------------------------------------------------------------------------- #
def write_pq(path: Path, values) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"pm25": pa.array(values, type=pa.float64())}), path)
    return path


def write_manifest(root: Path, archive: Path, *, version=1, extra=()) -> Path:
    manifest = root / "manifest.json"
    entries = []
    for p in sorted(archive.rglob("*.parquet")):
        st = p.stat()
        entries.append({
            "path": str(p.relative_to(archive)).replace("\\", "/"),
            "size": st.st_size, "mtime_ns": st.st_mtime_ns,
        })
    entries.extend(extra)
    tmp = manifest.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"version": version, "files": entries}))
    os.replace(tmp, manifest)
    return manifest


def make_settings(tmp_path: Path, archive: Path, manifest: Path) -> Settings:
    return Settings(
        archive_dir=archive, manifest_path=manifest, pm25_column="pm25",
        preview_max_files=50, meta_cache_size=256,
        sample_state_dir=tmp_path / "state",
    )


@pytest.fixture()
def env(tmp_path: Path):
    archive = tmp_path / "archive"
    write_pq(archive / "station=A/date=2026-01-01/part-01.parquet",
             [40.0, None, 50.0, 10.0, 35.0, None, 36.0])
    write_pq(archive / "station=A/date=2026-01-02/part-02.parquet",
             [100.0, 99.0, 1.0, None])
    write_pq(archive / "station=A/date=2026-02-01/part-03.parquet", [999.0])  # 月份外
    write_pq(archive / "station=B/date=2026-01-01/part-04.parquet", [77.0])   # 站点外
    manifest = write_manifest(tmp_path, archive)
    settings = make_settings(tmp_path, archive, manifest)
    return {
        "root": tmp_path, "archive": archive, "manifest": manifest,
        "settings": settings,
    }


def spec_for(**over):
    base = dict(
        station="A", month="2026-01", exceed_quota=3, missing_quota=2,
        exceed_seed="seed-exceed", missing_seed="seed-missing",
    )
    base.update(over)
    return build_spec(**base)


def client_for(settings: Settings) -> TestClient:
    return TestClient(create_app(settings))


def post_sample(client: TestClient, spec, path="/api/samples"):
    return client.post(path, json={
        "station": spec.station, "month": spec.month,
        "exceed_quota": spec.exceed_quota, "missing_quota": spec.missing_quota,
        "exceed_seed": spec.exceed_seed, "missing_seed": spec.missing_seed,
    })


# --------------------------------------------------------------------------- #
# 抽样语义
# --------------------------------------------------------------------------- #
def test_threshold_and_null_classification():
    assert classify(None) == KIND_MISSING
    assert classify(EXCEED_THRESHOLD) == KIND_EXCEED      # 恰好 35 计入超标
    assert classify(34.999) is None
    assert classify(0.0) is None


def test_sampling_selects_expected_rows_and_order(env):
    settings = env["settings"]
    snap = SnapshotStore(settings).current()
    spec = spec_for()
    sample = build_full(snap, spec)

    # 实际合格超标行：part-01 的 40/50/35/36 + part-02 的 100/99 = 6
    assert sample.eligible_counts[KIND_EXCEED] == 6
    assert sample.eligible_counts[KIND_MISSING] == 3

    exceed = sample.candidates[KIND_EXCEED]
    missing = sample.candidates[KIND_MISSING]
    assert len(exceed) == 3 and len(missing) == 2
    # 抽签顺序 = 抽签键升序
    assert [c.draw_key for c in exceed] == sorted(c.draw_key for c in exceed)
    # 每条都在范围内、类别正确、行号从 1 起
    for c in exceed:
        assert c.value is None or c.value >= EXCEED_THRESHOLD
        assert c.rel_path.startswith("station=A/date=2026-01-0")
        assert 1 <= c.row_number <= 7
    for c in missing:
        assert c.value is None
        assert 1 <= c.row_number <= 7
    # 抽签键可由种子 + 定位信息复算
    for c in exceed:
        assert c.draw_key == draw_key(
            spec.exceed_seed, c.rel_path, c.size, c.mtime_ns, c.row_number
        )


def test_sampling_is_deterministic_and_seed_sensitive(env):
    snap = SnapshotStore(env["settings"]).current()
    spec = spec_for()
    a = build_full(snap, spec)
    b = build_full(
        SnapshotStore(env["settings"]).current() if False else snap, spec
    )
    assert [c.locator for c in a.candidates[KIND_EXCEED]] == \
           [c.locator for c in b.candidates[KIND_EXCEED]]
    other = build_full(snap, spec_for(exceed_seed="another"))
    assert [c.locator for c in other.candidates[KIND_EXCEED]] != \
           [c.locator for c in a.candidates[KIND_EXCEED]]


def test_quota_larger_than_eligible_takes_all(env):
    snap = SnapshotStore(env["settings"]).current()
    spec = spec_for(exceed_quota=1000, missing_quota=1000)
    sample = build_full(snap, spec)
    assert len(sample.candidates[KIND_EXCEED]) == sample.eligible_counts[KIND_EXCEED]
    assert len(sample.candidates[KIND_MISSING]) == sample.eligible_counts[KIND_MISSING]


def test_zero_quota_returns_empty_list(env):
    snap = SnapshotStore(env["settings"]).current()
    sample = build_full(snap, spec_for(exceed_quota=0))
    assert sample.candidates[KIND_EXCEED] == []
    assert sample.eligible_counts[KIND_EXCEED] == 6   # 名额 0 也要如实统计合格记录


def test_per_file_candidates_never_exceed_quota(env):
    snap = SnapshotStore(env["settings"]).current()
    sample = build_full(snap, spec_for(exceed_quota=2, missing_quota=1))
    for items in sample.batches.values():
        for batch in items:
            assert len(batch.candidates) <= sample.spec.quotas[batch.kind]
            assert batch.row_count >= batch.eligible_rows
            # 抽签键升序 = 抽签顺序
            assert [c.draw_key for c in batch.candidates] == sorted(
                c.draw_key for c in batch.candidates
            )


def test_seed_shared_across_kinds_keeps_lists_independent(env):
    """两类可用同一种子；类别不同所以候选集合互不干扰。"""
    snap = SnapshotStore(env["settings"]).current()
    spec = spec_for(exceed_seed="same", missing_seed="same")
    sample = build_full(snap, spec)
    exceed_rows = {(c.rel_path, c.row_number) for c in sample.candidates[KIND_EXCEED]}
    missing_rows = {(c.rel_path, c.row_number) for c in sample.candidates[KIND_MISSING]}
    assert not exceed_rows & missing_rows
    assert len(sample.candidates[KIND_EXCEED]) == 3


def test_scan_file_matches_bruteforce_topk(tmp_path):
    """逐文件小顶堆结果必须与“全读后按抽签键排序取前 k”完全一致。"""
    path = write_pq(tmp_path / "x.parquet",
                    [100.0, None, 36.0, 10.0, None, 35.0, 88.0, 1.0])
    spec = spec_for(exceed_quota=2, missing_quota=1)
    size, mtime = path.stat().st_size, path.stat().st_mtime_ns
    batches = scan_file(path, "station=A/date=2026-01-01/x.parquet", size, mtime, spec)
    values = pq.read_table(path).column("pm25").to_pylist()
    for batch in batches:
        expected = []
        for idx, value in enumerate(values, start=1):
            value = None if value is None else float(value)
            if classify(value) != batch.kind:
                continue
            seed = spec.seeds[batch.kind]
            expected.append((
                draw_key(seed, batch.rel_path, size, mtime, idx), idx, value,
            ))
        expected.sort()
        expected = expected[:spec.quotas[batch.kind]]
        assert [(c.draw_key, c.row_number, c.value) for c in batch.candidates] == expected


def test_exceed_sampling_is_close_to_uniform(env):
    """同一种子下大量重复抽签，各行被选中的频率应大致均匀（无放回，每轮 2/6）。"""
    snap = SnapshotStore(env["settings"]).current()
    hits = Counter()
    trials = 300
    for i in range(trials):
        sample = build_full(snap, spec_for(exceed_quota=2, missing_quota=0,
                                           exceed_seed=f"seed-{i}"))
        for c in sample.candidates[KIND_EXCEED]:
            hits[(c.rel_path, c.row_number)] += 1
    assert len(hits) == 6  # 6 条合格行都出现过
    expected = trials * 2 / 6  # 每轮无放回取 2 / 6
    counts = list(hits.values())
    # 二项分布（n=300, p=1/3）标准差约 8.2，允许 ±3σ
    for count in counts:
        assert abs(count - expected) <= 3 * (trials * (1 / 3) * (2 / 3)) ** 0.5, (counts, hits)


# --------------------------------------------------------------------------- #
# 增量更新
# --------------------------------------------------------------------------- #
def _load_last(settings, station="A", month="2026-01"):
    return SampleStore(settings.sample_state_dir).load(station, month)


def test_generate_persists_and_survives_restart(env):
    settings = env["settings"]
    c1 = client_for(settings)
    body = post_sample(c1, spec_for())
    assert body.status_code == 200
    data = body.json()
    assert data["sample"]["source"] == "generate"
    assert len(data["sample"]["candidates"]["exceed"]) == 3

    # 新进程（新 TestClient / 新 SampleStore）仍能读回
    c2 = client_for(settings)
    last = c2.get("/api/samples/last", params={"station": "A", "month": "2026-01"})
    assert last.status_code == 200
    payload = last.json()
    assert payload["sample"]["snapshot_id"] == data["sample"]["snapshot_id"]
    assert payload["sample"]["candidates"] == data["sample"]["candidates"]
    assert payload["snapshot_still_current"] is True


def test_update_noop_reads_no_files(env):
    settings = env["settings"]
    client = client_for(settings)
    gen = post_sample(client, spec_for()).json()
    upd = post_sample(client, spec_for(), "/api/samples/update").json()
    assert upd["message"].startswith("清单未变化")
    assert upd["diff"]["read_files"] == []
    assert upd["diff"]["unchanged_snapshot"] is True
    assert upd["sample"]["candidates"] == gen["sample"]["candidates"]


def test_update_after_new_file_matches_full_draw(env):
    settings = env["settings"]
    client = client_for(settings)
    spec = spec_for()
    before = post_sample(client, spec).json()

    write_pq(env["archive"] / "station=A/date=2026-01-03/part-new.parquet",
             [100.0 + i for i in range(50)] + [None, 12.0])
    write_manifest(env["root"], env["archive"], version=2)
    stopped = Settings(**{**settings.__dict__})
    client2 = client_for(stopped)

    upd = post_sample(client2, spec, "/api/samples/update")
    assert upd.status_code == 200, upd.text
    data = upd.json()
    assert data["diff"]["read_files"] == [
        "station=A/date=2026-01-03/part-new.parquet"
    ]
    assert len(data["diff"]["reused_files"]) == 2

    # 与“按当前清单全量抽样”逐条相同
    snap = SnapshotStore(stopped).current()
    sample = _load_last(stopped)
    audit = compare_with_full(snap, sample)
    assert audit["equivalent"] is True

    # 变化清单：新增 + 撤回，且新增可能挤掉仍有效旧样本
    added = data["diff"]["added"]["exceed"]
    withdrawn = data["diff"]["withdrawn"]["exceed"]
    assert added, "新增文件应该贡献新的超标样本"
    assert withdrawn, "新增样本应挤掉抽签键更大的旧样本"
    assert all(c["path"].endswith("part-new.parquet") for c in added)
    old_keys = {c["draw_key"] for c in before["sample"]["candidates"]["exceed"]}
    new_keys = {c["draw_key"] for c in data["sample"]["candidates"]["exceed"]}
    assert len(new_keys - old_keys) == len(added)
    assert len(old_keys - new_keys) == len(withdrawn)


def test_update_after_file_rewrite_uses_new_identity(env):
    settings = env["settings"]
    client = client_for(settings)
    spec = spec_for(exceed_quota=10)   # 名额足够大，换版后两条新读数都应入样
    post_sample(client, spec)
    target = env["archive"] / "station=A/date=2026-01-01/part-01.parquet"
    time.sleep(0.01)
    write_pq(target, [200.0, 201.0, None])          # 换版：内容/大小/write 时间全变
    write_manifest(env["root"], env["archive"], version=2)

    client2 = client_for(settings)
    upd = post_sample(client2, spec, "/api/samples/update")
    assert upd.status_code == 200, upd.text
    data = upd.json()
    assert data["diff"]["read_files"] == [
        "station=A/date=2026-01-01/part-01.parquet"
    ]
    sample = _load_last(settings)
    values = {c.value for c in sample.candidates[KIND_EXCEED]
              if c.rel_path.endswith("part-01.parquet")}
    assert values == {200.0, 201.0}
    # 旧版本的 40/50/35/36 已随旧身份消失
    assert not values & {40.0, 50.0, 35.0, 36.0}
    assert compare_with_full(SnapshotStore(settings).current(), sample)["equivalent"]


def test_update_after_file_removed_withdraws_its_samples(env):
    settings = env["settings"]
    client = client_for(settings)
    spec = spec_for()
    post_sample(client, spec)
    (env["archive"] / "station=A/date=2026-01-02/part-02.parquet").unlink()
    write_manifest(env["root"], env["archive"], version=2)

    client2 = client_for(settings)
    upd = post_sample(client2, spec, "/api/samples/update")
    assert upd.status_code == 200, upd.text
    data = upd.json()
    assert data["diff"]["read_files"] == []
    assert all(not c["path"].endswith("part-02.parquet")
               for c in data["sample"]["candidates"]["exceed"])
    assert compare_with_full(SnapshotStore(settings).current(),
                             _load_last(settings))["equivalent"]


# --------------------------------------------------------------------------- #
# 失败语义：保留上次成功名单
# --------------------------------------------------------------------------- #
def test_update_fails_on_manifest_file_mismatch_and_keeps_last(env):
    settings = env["settings"]
    client = client_for(settings)
    spec = spec_for()
    good = post_sample(client, spec).json()

    entries = json.loads(env["manifest"].read_text())
    entries["files"][0]["size"] += 1      # 清单所记大小与磁盘不符
    tmp = env["manifest"].with_suffix(".json.tmp")
    tmp.write_text(json.dumps(entries))
    os.replace(tmp, env["manifest"])

    client2 = client_for(settings)
    bad = post_sample(client2, spec, "/api/samples/update")
    assert bad.status_code == 409
    detail = bad.json()["detail"]
    assert "FILE_MANIFEST_MISMATCH" in detail["reason_codes"]
    assert detail["last_success_snapshot_id"] == good["sample"]["snapshot_id"]

    # 上次成功名单原样可读，未被覆盖
    last = client2.get("/api/samples/last", params={"station": "A", "month": "2026-01"})
    assert last.status_code == 200
    payload = last.json()
    assert payload["sample"]["candidates"] == good["sample"]["candidates"]
    assert payload["last_attempt"]["outcome"] == "failed"
    assert "FILE_MANIFEST_MISMATCH" in payload["last_attempt"]["reason_codes"]


def test_update_fails_on_unreadable_file_and_keeps_last(env):
    settings = env["settings"]
    client = client_for(settings)
    spec = spec_for()
    good = post_sample(client, spec).json()
    corrupt = env["archive"] / "station=A/date=2026-01-04/part-corrupt.parquet"
    write_pq(corrupt, [80.0, 81.0])
    corrupt.write_bytes(corrupt.read_bytes()[: 40])   # 截断 footer
    write_manifest(env["root"], env["archive"], version=2)

    client2 = client_for(settings)
    bad = post_sample(client2, spec, "/api/samples/update")
    assert bad.status_code == 409
    assert "FILE_UNREADABLE" in bad.json()["detail"]["reason_codes"]
    last = client2.get("/api/samples/last", params={"station": "A", "month": "2026-01"})
    assert last.json()["sample"]["candidates"] == good["sample"]["candidates"]


def test_update_rejects_missing_file_and_keeps_last(env):
    settings = env["settings"]
    client = client_for(settings)
    spec = spec_for()
    post_sample(client, spec)
    ghost = "station=A/date=2026-01-05/ghost.parquet"
    write_manifest(env["root"], env["archive"], version=2,
                   extra=[{"path": ghost, "size": 10, "mtime_ns": 20}])
    client2 = client_for(settings)
    bad = post_sample(client2, spec, "/api/samples/update")
    assert bad.status_code == 409
    assert "FILE_MISSING" in bad.json()["detail"]["reason_codes"]


def test_update_requires_previous_sample(env):
    client = client_for(env["settings"])
    r = post_sample(client, spec_for(), "/api/samples/update")
    assert r.status_code == 404
    assert r.json()["detail"]["reason_codes"] == ["NO_PREVIOUS_SAMPLE"]


def test_update_rejects_changed_spec(env):
    settings = env["settings"]
    client = client_for(settings)
    post_sample(client, spec_for())
    bad = post_sample(client, spec_for(exceed_seed="changed"), "/api/samples/update")
    assert bad.status_code == 409
    assert "SAMPLE_SPEC_CHANGED" in bad.json()["detail"]["reason_codes"]


def test_generate_fails_when_manifest_missing(env, tmp_path):
    bad_settings = make_settings(tmp_path, env["archive"], tmp_path / "nope.json")
    client = client_for(bad_settings)
    r = post_sample(client, spec_for())
    assert r.status_code == 503
    assert r.json()["detail"]["error"] == "MANIFEST_UNAVAILABLE"


def test_generate_fails_on_invalid_manifest_json(env):
    env["manifest"].write_text("{ not json")
    client = client_for(env["settings"])
    r = post_sample(client, spec_for())
    assert r.status_code == 503
    assert r.json()["detail"]["error"] == "MANIFEST_INVALID"


def test_generate_fails_on_manifest_path_traversal(env):
    write_manifest(env["root"], env["archive"], version=2,
                   extra=[{"path": "../../etc/passwd", "size": 1, "mtime_ns": 1}])
    client = client_for(env["settings"])
    r = post_sample(client, spec_for())
    assert r.status_code == 503
    assert r.json()["detail"]["error"] == "MANIFEST_CHANGED"


def test_generate_requires_manifest_mode(archive, tmp_path):
    settings = Settings(
        archive_dir=archive, manifest_path=None, pm25_column="pm25",
        preview_max_files=50, meta_cache_size=64,
        sample_state_dir=tmp_path / "state",
    )
    client = client_for(settings)
    r = post_sample(client, spec_for())
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "SAMPLE_REQUIRES_MANIFEST"


def test_manifest_changed_mid_read_is_rejected(env, monkeypatch):
    """读取期间清单被替换：整次操作中止，不能产生混合版本结果。"""
    settings = env["settings"]
    client = client_for(settings)
    spec = spec_for()
    good = post_sample(client, spec).json()

    write_pq(env["archive"] / "station=A/date=2026-01-06/part-race.parquet", [90.0])

    from pm25_planner import api as api_module

    original = api_module.build_update

    def racing_update(*args, **kwargs):
        result = original(*args, **kwargs)
        write_manifest(env["root"], env["archive"], version=99)  # 期间再次换版
        return result

    monkeypatch.setattr(api_module, "build_update", racing_update)
    r = post_sample(client, spec, "/api/samples/update")
    assert r.status_code == 409
    assert "MANIFEST_CHANGED" in r.json()["detail"]["reason_codes"]
    # 上次成功名单未被写坏
    last = client.get("/api/samples/last", params={"station": "A", "month": "2026-01"})
    assert last.json()["sample"]["candidates"] == good["sample"]["candidates"]


def test_sample_store_state_corruption_reports_reason(env):
    settings = env["settings"]
    store = SampleStore(settings.sample_state_dir)
    path = settings.sample_state_dir / "A__2026-01" / "last_success.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{broken")
    with pytest.raises(SampleError) as exc:
        store.load("A", "2026-01")
    assert "SAMPLE_STATE_CORRUPT" in exc.value.reason_codes


def test_incremental_limit(env):
    settings = env["settings"]
    client = client_for(settings)
    spec = spec_for()
    post_sample(client, spec)
    for i in range(3):
        write_pq(env["archive"] / f"station=A/date=2026-01-1{i}/part-many.parquet",
                 [60.0, None])
    write_manifest(env["root"], env["archive"], version=2)
    limited = Settings(**{**settings.__dict__, "sample_max_incremental_files": 2})
    r = post_sample(client_for(limited), spec, "/api/samples/update")
    assert r.status_code == 409
    assert "INCREMENTAL_LIMIT_EXCEEDED" in r.json()["detail"]["reason_codes"]


# --------------------------------------------------------------------------- #
# 其它
# --------------------------------------------------------------------------- #
def test_month_bounds_and_invalid_month():
    first, nxt = month_bounds("2026-12")
    assert first.isoformat() == "2026-12-01"
    assert nxt.isoformat() == "2027-01-01"
    with pytest.raises(SampleError):
        month_bounds("2026-13")


def test_audit_endpoint_reports_equivalence(env):
    settings = env["settings"]
    client = client_for(settings)
    spec = spec_for()
    post_sample(client, spec)
    r = post_sample(client, spec, "/api/samples/audit")
    assert r.status_code == 200
    body = r.json()
    assert body["equivalent"] is True
    assert body["details"]["exceed"]["missing"] == 0


def test_list_saved_samples(env):
    client = client_for(env["settings"])
    post_sample(client, spec_for())
    r = client.get("/api/samples")
    assert r.status_code == 200
    items = r.json()
    assert items and items[0]["station"] == "A" and items[0]["month"] == "2026-01"
    assert items[0]["counts"]["exceed"] == 3


def test_snapshot_rejects_manifest_changed_during_read(archive, tmp_path, monkeypatch):
    manifest = write_manifest(tmp_path, archive)
    settings = make_settings(tmp_path, archive, manifest)
    store = SnapshotStore(settings)
    store.current()  # 预热
    store.invalidate()

    real_open = Path.open

    def racing_open(self, *args, **kwargs):
        fh = real_open(self, *args, **kwargs)
        if self == manifest:
            st = manifest.stat()
            os.utime(manifest, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))
        return fh

    monkeypatch.setattr(Path, "open", racing_open)
    with pytest.raises(ManifestError):
        store.current()
