"""FastAPI 应用：读取计划的生成、快照核对与预览执行。"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import Settings
from .models import (
    PlanRequest,
    PlanResponse,
    PreviewResponse,
    SavedSampleItem,
    SampleAuditResponse,
    SampleDiffView,
    SampleGenerateResponse,
    SampleLastResponse,
    SampleListResponse,
    SampleSpecRequest,
    SampleUpdateRequest,
    SnapshotResponse,
    VerifyRequest,
    VerifyResponse,
)
from .parquet_meta import duckdb_cross_check
from .planner import build_plan
from .reader import execute_preview
from .sampling import (
    KINDS,
    REASON_MANIFEST_CHANGED,
    REASON_MANIFEST_INVALID,
    SampleError,
    SampleList,
    SampleStore,
    build_full,
    build_spec,
    build_update,
    compare_with_full,
    diff_samples,
)
from .snapshot import ManifestError, SnapshotStore

WEB_DIR = Path(__file__).resolve().parents[2] / "web"


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    store = SnapshotStore(settings)
    app = FastAPI(
        title="PM2.5 Parquet 档案读取计划服务",
        version="1.0.0",
        description="基于分区值与 min/max/null_count 元数据生成可核对的文件读取计划，"
                    "统计缺失/损坏一律保守保留，计划绑定档案快照。",
    )
    app.state.settings = settings
    app.state.store = store
    sample_store = SampleStore(settings.sample_state_dir)
    # 生成/更新会读-改-写“上次成功名单”，串行化避免并发请求互相覆盖
    sample_lock = threading.Lock()

    def _snapshot_or_503():
        try:
            return store.current()
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"清单文件不可用（{exc.filename}），请先部署清单后重试",
            )
        except ManifestError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"清单读取失败：{exc}",
            )
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"清单 JSON 不完整或损坏: {exc.msg}（请确认替换是否原子完成）",
            )

    @app.get("/api/health")
    def health() -> dict:
        try:
            snap = store.current()
            return {"status": "ok", "snapshot_id": snap.snapshot_id,
                    "file_count": len(snap.files), "source": snap.source}
        except Exception as exc:  # noqa: BLE001 - 健康检查需要兜住全部启动期异常
            return {"status": "degraded", "error": f"{exc.__class__.__name__}: {exc}"}

    @app.get("/api/snapshot", response_model=SnapshotResponse)
    def get_snapshot() -> SnapshotResponse:
        snap = _snapshot_or_503()
        return SnapshotResponse(
            snapshot_id=snap.snapshot_id, source=snap.source,  # type: ignore[arg-type]
            file_count=len(snap.files),
            total_bytes=sum(f.size for f in snap.files),
            archive_dir=str(snap.archive_dir),
            manifest_path=str(snap.manifest_path) if snap.manifest_path else None,
            generated_at=snap.generated_at,
        )

    @app.post("/api/plan", response_model=PlanResponse)
    def create_plan(req: PlanRequest) -> PlanResponse:
        snap = _snapshot_or_503()
        if req.expected_snapshot_id and req.expected_snapshot_id != snap.snapshot_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "STALE_SNAPSHOT",
                    "message": "清单在计划生成前已替换，旧快照绑定失效，请重新查询",
                    "expected_snapshot_id": req.expected_snapshot_id,
                    "current_snapshot_id": snap.snapshot_id,
                },
            )
        return build_plan(snap, req)

    @app.post("/api/plan/verify", response_model=VerifyResponse)
    def verify_snapshot(req: VerifyRequest) -> VerifyResponse:
        snap = _snapshot_or_503()
        if req.snapshot_id == snap.snapshot_id:
            return VerifyResponse(
                valid=True, current_snapshot_id=snap.snapshot_id,
                message="快照一致，计划仍然绑定当前档案",
            )
        return VerifyResponse(
            valid=False, current_snapshot_id=snap.snapshot_id,
            message="档案清单已替换，该计划绑定的快照已失效，请重新生成计划",
        )

    @app.post("/api/plan/preview", response_model=PreviewResponse)
    def preview_plan(req: PlanRequest) -> PreviewResponse:
        snap = _snapshot_or_503()
        bound_id = req.expected_snapshot_id or snap.snapshot_id
        # 预览内部再次按当前快照判定；快照变化时 valid_snapshot=False
        return execute_preview(snap, bound_id, req, settings)

    @app.get("/api/debug/duckdb-stats")
    def debug_duckdb_stats(path: str) -> dict:
        """调试接口：对单个文件用 DuckDB 独立重算统计，便于与 PyArrow 结果核对。"""
        root = settings.archive_dir.resolve()
        target = (root / path).resolve()
        if root not in target.parents and target != root:
            raise HTTPException(status_code=400, detail="路径越界，拒绝访问")
        if not target.exists():
            raise HTTPException(status_code=404, detail=f"文件不存在: {path}")
        result = duckdb_cross_check(target, settings.pm25_column)
        arrow_meta = store.current().by_relpath().get(path.replace("\\", "/").lstrip("./"))
        return {
            "path": path,
            "duckdb": result,
            "pyarrow": None if arrow_meta is None else {
                "pm25_min": arrow_meta.pm25_min,
                "pm25_max": arrow_meta.pm25_max,
                "null_count": arrow_meta.null_count,
                "row_count": arrow_meta.row_count,
                "stats_status": arrow_meta.stats_status,
            },
        }

    # ------------------------------------------------------------------ #
    # 月报抽查名单（清单模式）
    # ------------------------------------------------------------------ #
    def _candidate_view(candidate) -> dict:
        payload = candidate.to_json()
        # mtime_ns 超出 JS Number 安全整数范围，按十进制字符串返回
        payload["mtime_ns"] = str(payload["mtime_ns"])
        return payload

    def _candidate_view_from_json(payload: dict) -> dict:
        out = dict(payload)
        out["mtime_ns"] = str(out["mtime_ns"])
        return out

    def _sample_view(sample: SampleList) -> SampleListResponse:
        files = []
        for rel in sorted(sample.batches):
            for batch in sample.batches[rel]:
                files.append({
                    "path": batch.rel_path,
                    "size": batch.size,
                    "mtime_ns": str(batch.mtime_ns),
                    "kind": batch.kind,
                    "row_count": batch.row_count,
                    "eligible_rows": batch.eligible_rows,
                    "kept_candidates": len(batch.candidates),
                })
        return SampleListResponse(
            spec=sample.spec.to_json(),
            snapshot_id=sample.snapshot_id,
            snapshot=sample.snapshot,
            generated_at=sample.generated_at,
            source=sample.source,
            previous_snapshot_id=sample.previous_snapshot_id,
            candidates={
                kind: [_candidate_view(c) for c in sample.candidates[kind]]
                for kind in KINDS
            },
            eligible_counts=sample.eligible_counts,
            notes=sample.notes,
            files=files,
            quotas=sample.spec.quotas,
        )

    def _diff_view(payload: dict | None) -> SampleDiffView | None:
        if payload is None:
            return None
        return SampleDiffView(
            added={k: [_candidate_view_from_json(c)
                       for c in payload.get("added", {}).get(k, [])]
                   for k in KINDS},
            withdrawn={k: [_candidate_view_from_json(c)
                           for c in payload.get("withdrawn", {}).get(k, [])]
                       for k in KINDS},
            reused_files=payload.get("reused_files", []),
            read_files=payload.get("read_files", []),
            unchanged_snapshot=bool(payload.get("unchanged_snapshot", False)),
        )

    def _last_or_none(station: str, month: str) -> SampleList | None:
        try:
            return sample_store.load(station, month)
        except SampleError as exc:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "SAMPLE_STATE_CORRUPT",
                    "reason_codes": exc.reason_codes,
                    "message": str(exc),
                    "details": exc.details,
                },
            )

    def _fail_sample(
        exc: SampleError, spec_req: SampleSpecRequest, snap=None, status: int = 409
    ) -> HTTPException:
        """失败时保留上次成功名单：响应中带上它的快照与时间，便于页面回显。"""
        previous = None
        try:
            previous = sample_store.load(spec_req.station, spec_req.month)
        except SampleError:
            previous = None
        return HTTPException(
            status_code=status,
            detail={
                "error": "SAMPLE_FAILED",
                "reason_codes": exc.reason_codes,
                "message": str(exc),
                "details": exc.details,
                "last_success_snapshot_id": previous.snapshot_id if previous else None,
                "last_success_generated_at": previous.generated_at if previous else None,
                "current_snapshot_id": snap.snapshot_id if snap is not None else None,
            },
        )

    def _record_attempt(spec_req: SampleSpecRequest, outcome: str, exc: SampleError | None = None) -> None:
        try:
            sample_store.save_attempt(
                spec_req.station, spec_req.month,
                {
                    "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "outcome": outcome,
                    "reason_codes": exc.reason_codes if exc else [],
                    "message": str(exc) if exc else None,
                    "details": exc.details if exc else [],
                    "action": "generate" if isinstance(spec_req, SampleSpecRequest)
                    and not isinstance(spec_req, SampleUpdateRequest) else "update",
                },
            )
        except OSError:
            # 尝试记录失败不影响主结果
            pass

    def _snapshot_for_sample():
        # 生成/更新名单必须在清单模式：否则“清单换版”语义不存在
        if settings.manifest_path is None:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "SAMPLE_REQUIRES_MANIFEST",
                    "reason_codes": ["MANIFEST_MODE_REQUIRED"],
                    "message": "抽查名单只支持清单模式：请设置 PM25_MANIFEST_PATH 后重启",
                    "details": [],
                },
            )
        try:
            return store.current()
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "MANIFEST_UNAVAILABLE",
                    "reason_codes": ["MANIFEST_MISSING"],
                    "message": f"清单文件不可用（{exc.filename}）",
                    "details": [],
                },
            )
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "MANIFEST_INVALID",
                    "reason_codes": [REASON_MANIFEST_INVALID],
                    "message": f"清单 JSON 不完整或损坏: {exc.msg}",
                    "details": [],
                },
            )
        except ManifestError as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "MANIFEST_CHANGED",
                    "reason_codes": [REASON_MANIFEST_CHANGED],
                    "message": str(exc),
                    "details": [],
                },
            )

    def _spec_or_400(req: SampleSpecRequest):
        try:
            return build_spec(
                station=req.station, month=req.month,
                exceed_quota=req.exceed_quota, missing_quota=req.missing_quota,
                exceed_seed=req.exceed_seed, missing_seed=req.missing_seed,
            )
        except SampleError as exc:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "INVALID_SAMPLE_SPEC",
                    "reason_codes": exc.reason_codes,
                    "message": str(exc),
                    "details": exc.details,
                },
            )

    def _ensure_snapshot_stable(snap) -> None:
        """读文件期间清单若被再次替换，本次结果可能混合两个版本，必须中止。"""
        try:
            latest = store.current()
        except Exception as exc:  # noqa: BLE001 - 清单变得不可用同样视为换版失败
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "MANIFEST_CHANGED",
                    "reason_codes": [REASON_MANIFEST_CHANGED],
                    "message": f"操作期间清单变得不可用，已中止：{exc.__class__.__name__}: {exc}",
                    "details": [],
                },
            )
        if latest.snapshot_id != snap.snapshot_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "SAMPLE_FAILED",
                    "reason_codes": [REASON_MANIFEST_CHANGED],
                    "message": (
                        "清单在名单生成/更新过程中再次换版"
                        f"（开始 {snap.snapshot_id} → 结束 {latest.snapshot_id}），"
                        "已中止并保留上次成功名单；请重试"
                    ),
                    "details": [],
                    "current_snapshot_id": latest.snapshot_id,
                },
            )

    @app.post("/api/samples", response_model=SampleGenerateResponse)
    def generate_sample(req: SampleSpecRequest) -> SampleGenerateResponse:
        with sample_lock:
            return _generate_sample_locked(req)

    def _generate_sample_locked(req: SampleSpecRequest) -> SampleGenerateResponse:
        snap = _snapshot_for_sample()
        spec = _spec_or_400(req)
        previous = _last_or_none(req.station, req.month)
        try:
            sample = build_full(snap, spec, settings.pm25_column)
        except SampleError as exc:
            _record_attempt(req, "failed", exc)
            raise _fail_sample(exc, req, snap)
        _ensure_snapshot_stable(snap)
        sample_store.save(sample)
        diff = None
        if previous is not None and previous.spec == spec:
            # 同参数重新生成时，同样标出相对“上次成功名单”的新增/撤回
            diff = diff_samples(previous, sample)
        sample_store.save_diff(req.station, req.month, diff or {
            "added": {k: [] for k in KINDS},
            "withdrawn": {k: [] for k in KINDS},
            "reused_files": [],
            "read_files": sorted(sample.batches),
            "unchanged_snapshot": False,
        })
        _record_attempt(req, "success")
        return SampleGenerateResponse(
            sample=_sample_view(sample), diff=_diff_view(diff),
            message="名单已生成并保存为“上次成功名单”",
        )

    @app.post("/api/samples/update", response_model=SampleGenerateResponse)
    def update_sample(req: SampleUpdateRequest) -> SampleGenerateResponse:
        with sample_lock:
            return _update_sample_locked(req)

    def _update_sample_locked(req: SampleUpdateRequest) -> SampleGenerateResponse:
        snap = _snapshot_for_sample()
        spec = _spec_or_400(req)
        previous = _last_or_none(req.station, req.month)
        if previous is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "NO_PREVIOUS_SAMPLE",
                    "reason_codes": ["NO_PREVIOUS_SAMPLE"],
                    "message": "没有上次成功名单，无法增量更新；请先“生成名单”",
                    "details": [],
                    "current_snapshot_id": snap.snapshot_id,
                },
            )
        try:
            result = build_update(
                snap, previous, spec,
                pm25_column=settings.pm25_column,
                max_incremental_files=settings.sample_max_incremental_files,
            )
        except SampleError as exc:
            _record_attempt(req, "failed", exc)
            raise _fail_sample(exc, req, snap)
        _ensure_snapshot_stable(snap)
        sample_store.save(result.sample)
        sample_store.save_diff(req.station, req.month, result.to_json())
        _record_attempt(req, "success")
        return SampleGenerateResponse(
            sample=_sample_view(result.sample),
            diff=_diff_view(result.to_json()),
            message=(
                "清单未变化，名单保持不变（未实读文件）"
                if result.unchanged_snapshot
                else f"已更新名单：实读 {len(result.read_files)} 个新增/换版文件"
            ),
        )

    @app.get("/api/samples/last", response_model=SampleLastResponse)
    def last_sample(station: str, month: str) -> SampleLastResponse:
        sample = _last_or_none(station, month)
        if sample is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "NO_SAVED_SAMPLE",
                    "reason_codes": ["NO_SAVED_SAMPLE"],
                    "message": f"站点 {station} 月份 {month} 尚无已保存名单",
                    "details": [],
                },
            )
        current_id = None
        try:
            current_id = store.current().snapshot_id
        except Exception:  # noqa: BLE001 - 清单暂不可用不影响查看已保存名单
            current_id = None
        try:
            diff = sample_store.load_diff(station, month)
        except SampleError:
            diff = None
        try:
            attempt = sample_store.load_attempt(station, month)
        except SampleError:
            attempt = None
        return SampleLastResponse(
            sample=_sample_view(sample),
            diff=_diff_view(diff),
            last_attempt=attempt,
            snapshot_still_current=(
                current_id is not None and current_id == sample.snapshot_id
            ),
            current_snapshot_id=current_id,
        )

    @app.get("/api/samples", response_model=list[SavedSampleItem])
    def list_samples() -> list[SavedSampleItem]:
        return [SavedSampleItem(**item) for item in sample_store.list_saved()]

    @app.post("/api/samples/audit", response_model=SampleAuditResponse)
    def audit_sample(req: SampleSpecRequest) -> SampleAuditResponse:
        """全量重抽一遍并逐条比对已保存名单（会读范围内全部文件，用于核对）。"""
        snap = _snapshot_for_sample()
        spec = _spec_or_400(req)
        sample = _last_or_none(req.station, req.month)
        if sample is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "NO_SAVED_SAMPLE",
                    "reason_codes": ["NO_SAVED_SAMPLE"],
                    "message": "尚无已保存名单，无法核对",
                    "details": [],
                },
            )
        if sample.spec != spec:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "INVALID_SAMPLE_SPEC",
                    "reason_codes": ["SAMPLE_SPEC_CHANGED"],
                    "message": "核对参数与已保存名单不一致",
                    "details": [f"已保存: {sample.spec.to_json()}", f"本次: {spec.to_json()}"],
                },
            )
        try:
            result = compare_with_full(snap, sample, settings.pm25_column)
        except SampleError as exc:
            raise _fail_sample(exc, req, snap)
        return SampleAuditResponse(
            **result,
            rows_read=sum(
                items[0].row_count for items in sample.batches.values() if items
            ),
            notes=[
                "全量比对会实读当前清单范围内全部文件；结果一致表示更新名单"
                "与“按当前清单全量抽样”逐条相同"
            ],
        )

    # 静态前端（index.html 在根路径提供）
    app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(str(WEB_DIR / "index.html"))

    return app


app = create_app()
