"""监测月报抽查名单：确定性、可增量更新的分层等概率无放回抽样。

口径
====
- 站点 + 月份（YYYY-MM）为一个抽样范围；
- 两类名额：``exceed``（PM2.5 ≥ 35 μg/m³）、``missing``（缺测 null）。
  一行只归入一类：null 记缺测，非空且 ≥35 记超标，其余不入样；
- 两类各自等概率无放回抽取，不足名额则全取；两类使用各自的种子。

“抽签顺序”如何固定
==================
每一条记录（行）的抽签键为::

    draw_key = HMAC-SHA256(seed, rel_path \0 size \0 mtime_ns \0 row_number)

其中 ``rel_path`` / ``size`` / ``mtime_ns`` 就是清单所记的定位信息，
``row_number`` 是文件内行号（从 1 开始）。因此同一文件、同一种子下每行的
抽签键永久固定；把范围内全部合格行的抽签键升序排列，取前 ``quota`` 个，
就是在全部合格记录上等概率无放回抽样（随机排列的前 quota 个）。

增量更新为何必然与全量抽样一致
==============================
全局结果只取决于“所有合格行的抽签键”，而每个文件的抽签键只由该文件自身
的 ``(seed, rel_path, size, mtime_ns)`` 与该文件内容（行号/读数）决定。
因此：

1. 文件未变（清单声明的 path/size/mtime_ns 均相同且磁盘核对一致）：
   复用上次成功名单中该文件的候选即可，无需再读；
2. 文件新增或换版（任一字段变化）：按新身份重新计算该文件的抽签键；
3. 合并时每个文件每类最多保留 ``quota`` 个候选（文件对最终名单的
   贡献不可能超过全局名额），再在合并后的候选里取抽签键最小的 ``quota`` 个。

这与“按当前清单把范围内全部文件读一遍再取前 quota 个”的结果**逐条相同**，
因为参与排序的键集合和取值规则完全一致。新增文件带来更小的抽签键时，
会自然挤掉仍有效的旧样本；旧文件被撤回时其候选整体消失。

失败语义
========
清单换版（读取期间被并发替换）、文件与清单不符、文件缺失、Parquet 读取失败、
清单 JSON 非法等任何异常都会抛出 :class:`SampleError`，由 API 返回明确原因；
调用方**保留上次成功名单**，不会写入半成品。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq

from .parquet_meta import FileMeta
from .snapshot import Snapshot

EXCEED_THRESHOLD = 35.0
KIND_EXCEED = "exceed"
KIND_MISSING = "missing"
KINDS = (KIND_EXCEED, KIND_MISSING)

# 原因码：随失败响应返回，供页面与自动化核对
REASON_MANIFEST_CHANGED = "MANIFEST_CHANGED"
REASON_MANIFEST_INVALID = "MANIFEST_INVALID"
REASON_FILE_MISMATCH = "FILE_MANIFEST_MISMATCH"
REASON_FILE_MISSING = "FILE_MISSING"
REASON_FILE_UNREADABLE = "FILE_UNREADABLE"
REASON_NO_FILES = "NO_FILES_IN_RANGE"
REASON_SPEC_CHANGED = "SAMPLE_SPEC_CHANGED"
REASON_LIMIT = "INCREMENTAL_LIMIT_EXCEEDED"
REASON_STATE_CORRUPT = "SAMPLE_STATE_CORRUPT"
REASON_INVALID_SPEC = "INVALID_SPEC"


class SampleError(RuntimeError):
    """抽样/更新失败；调用方据此保留上次成功名单。"""

    def __init__(
        self, message: str, reason_codes: list[str], details: list[str] | None = None
    ) -> None:
        super().__init__(message)
        self.reason_codes = list(dict.fromkeys(reason_codes))
        self.details = details or []


# --------------------------------------------------------------------------- #
# 数据模型
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SampleSpec:
    station: str
    month: str  # YYYY-MM
    exceed_quota: int
    missing_quota: int
    exceed_seed: str
    missing_seed: str

    def to_json(self) -> dict:
        return {
            "station": self.station,
            "month": self.month,
            "exceed_quota": self.exceed_quota,
            "missing_quota": self.missing_quota,
            "exceed_seed": self.exceed_seed,
            "missing_seed": self.missing_seed,
        }

    @staticmethod
    def from_json(payload: dict) -> "SampleSpec":
        return SampleSpec(
            station=str(payload["station"]),
            month=str(payload["month"]),
            exceed_quota=int(payload["exceed_quota"]),
            missing_quota=int(payload["missing_quota"]),
            exceed_seed=str(payload["exceed_seed"]),
            missing_seed=str(payload["missing_seed"]),
        )

    @property
    def quotas(self) -> dict[str, int]:
        return {KIND_EXCEED: self.exceed_quota, KIND_MISSING: self.missing_quota}

    @property
    def seeds(self) -> dict[str, str]:
        return {KIND_EXCEED: self.exceed_seed, KIND_MISSING: self.missing_seed}

    @property
    def shared_seed(self) -> bool:
        return self.exceed_seed == self.missing_seed


@dataclass(frozen=True)
class Candidate:
    kind: str
    station: str
    month: str
    rel_path: str
    size: int
    mtime_ns: int
    row_number: int  # 文件内行号，从 1 开始
    value: float | None
    draw_key: str  # 32 字节 HMAC 的十六进制

    @property
    def locator(self) -> tuple[str, int, int, int]:
        return (self.rel_path, self.size, self.mtime_ns, self.row_number)

    def to_json(self) -> dict:
        return {
            "kind": self.kind,
            "station": self.station,
            "month": self.month,
            "path": self.rel_path,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "row_number": self.row_number,
            "value": self.value,
            "draw_key": self.draw_key,
        }

    @staticmethod
    def from_json(payload: dict, kind: str | None = None) -> "Candidate":
        return Candidate(
            kind=payload.get("kind", kind),
            station=str(payload["station"]),
            month=str(payload["month"]),
            rel_path=str(payload["path"]),
            size=int(payload["size"]),
            mtime_ns=int(payload["mtime_ns"]),
            row_number=int(payload["row_number"]),
            value=None if payload.get("value") is None else float(payload["value"]),
            draw_key=str(payload["draw_key"]),
        )


@dataclass(frozen=True)
class FileBatch:
    """单文件单类别的抽签证据（候选数不超过该类名额）。"""

    rel_path: str
    size: int
    mtime_ns: int
    kind: str
    row_count: int
    eligible_rows: int
    candidates: tuple[Candidate, ...]  # 按抽签键升序

    @property
    def identity(self) -> str:
        return file_identity(self.rel_path, self.size, self.mtime_ns)

    def to_json(self) -> dict:
        return {
            "path": self.rel_path,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "kind": self.kind,
            "row_count": self.row_count,
            "eligible_rows": self.eligible_rows,
            "candidates": [c.to_json() for c in self.candidates],
        }

    @staticmethod
    def from_json(payload: dict) -> "FileBatch":
        kind = str(payload["kind"])
        return FileBatch(
            rel_path=str(payload["path"]),
            size=int(payload["size"]),
            mtime_ns=int(payload["mtime_ns"]),
            kind=kind,
            row_count=int(payload["row_count"]),
            eligible_rows=int(payload["eligible_rows"]),
            candidates=tuple(
                Candidate.from_json(c, kind) for c in payload.get("candidates", [])
            ),
        )


@dataclass
class SampleList:
    """一份成功生成的名单，连同生成它时的快照证据。"""

    spec: SampleSpec
    snapshot_id: str
    snapshot: dict
    generated_at: str
    source: Literal["generate", "update", "update_noop"]
    batches: dict[str, tuple[FileBatch, ...]]  # rel_path -> (exceed, missing)
    candidates: dict[str, list[Candidate]]
    eligible_counts: dict[str, int]
    notes: list[str]
    previous_snapshot_id: str | None = None

    @property
    def manifest_version(self) -> str | None:
        return self.snapshot.get("manifest_version")

    def to_json(self) -> dict:
        return {
            "format": 2,
            "spec": self.spec.to_json(),
            "snapshot_id": self.snapshot_id,
            "snapshot": self.snapshot,
            "generated_at": self.generated_at,
            "source": self.source,
            "previous_snapshot_id": self.previous_snapshot_id,
            "eligible_counts": self.eligible_counts,
            "notes": self.notes,
            "batches": [
                b.to_json()
                for rel in sorted(self.batches)
                for b in self.batches[rel]
            ],
            "candidates": {
                kind: [c.to_json() for c in self.candidates[kind]] for kind in KINDS
            },
        }

    @staticmethod
    def from_json(payload: dict) -> "SampleList":
        if int(payload.get("format", 0)) != 2:
            raise SampleError(
                f"已保存名单的状态格式不受支持: {payload.get('format')!r}",
                [REASON_STATE_CORRUPT],
            )
        batches: dict[str, list[FileBatch]] = {}
        for raw in payload.get("batches", []):
            batch = FileBatch.from_json(raw)
            batches.setdefault(batch.rel_path, []).append(batch)
        restored: dict[str, tuple[FileBatch, ...]] = {}
        for rel, items in batches.items():
            by_kind = {b.kind: b for b in items}
            if set(by_kind) != set(KINDS):
                raise SampleError(
                    f"已保存名单中文件 {rel} 的证据类别不完整",
                    [REASON_STATE_CORRUPT],
                )
            restored[rel] = tuple(by_kind[k] for k in KINDS)
        return SampleList(
            spec=SampleSpec.from_json(payload["spec"]),
            snapshot_id=str(payload["snapshot_id"]),
            snapshot=dict(payload.get("snapshot", {})),
            generated_at=str(payload["generated_at"]),
            source=payload.get("source", "generate"),
            batches=restored,
            candidates={
                kind: [Candidate.from_json(c, kind)
                       for c in payload.get("candidates", {}).get(kind, [])]
                for kind in KINDS
            },
            eligible_counts={
                k: int(v) for k, v in payload.get("eligible_counts", {}).items()
            },
            notes=list(payload.get("notes", [])),
            previous_snapshot_id=payload.get("previous_snapshot_id"),
        )


# --------------------------------------------------------------------------- #
# 确定性抽签
# --------------------------------------------------------------------------- #
def file_identity(rel_path: str, size: int, mtime_ns: int) -> str:
    return f"{rel_path}\x00{size}\x00{mtime_ns}"


def draw_key(seed: str, rel_path: str, size: int, mtime_ns: int, row_number: int) -> str:
    """固定某一行抽签顺序的键：种子 + 清单定位信息 + 文件内行号。"""
    message = (
        f"{rel_path}\x00{size}\x00{mtime_ns}\x00{row_number}".encode("utf-8")
    )
    return hmac.new(seed.encode("utf-8"), message, hashlib.sha256).hexdigest()


def classify(value: float | None) -> str | None:
    if value is None:
        return KIND_MISSING
    if value >= EXCEED_THRESHOLD:
        return KIND_EXCEED
    return None


def _resolve_inside(root: Path, rel_path: str) -> Path:
    root_resolved = root.resolve()
    target = (root_resolved / rel_path).resolve()
    if target != root_resolved and root_resolved not in target.parents:
        raise SampleError(
            f"清单路径越出档案根目录: {rel_path}", [REASON_MANIFEST_INVALID]
        )
    return target


def check_file(
    meta: FileMeta, declared: tuple[int, int], root: Path
) -> tuple[str, str | None]:
    """核对单个清单文件，返回 (status, error)。status ∈ ok/mismatch/missing/unreadable。"""
    size, mtime_ns = declared
    try:
        full = _resolve_inside(root, meta.rel_path)
    except SampleError as exc:
        return "mismatch", str(exc)
    if not full.exists():
        return "missing", "清单引用的文件在磁盘上不存在"
    try:
        st = full.stat()
    except OSError as exc:
        return "unreadable", f"文件无法访问: {exc.__class__.__name__}: {exc}"
    if st.st_size != size or st.st_mtime_ns != mtime_ns:
        return (
            "mismatch",
            f"磁盘文件与清单不符：清单 size={size}/mtime_ns={mtime_ns}，"
            f"实际 size={st.st_size}/mtime_ns={st.st_mtime_ns}",
        )
    if meta.stats_status == "unreadable":
        return "unreadable", f"Parquet footer 不可读: {meta.error}"
    if meta.stats_status == "missing_column":
        return "unreadable", f"缺少 PM2.5 列: {meta.error}"
    return "ok", None


def scan_file(
    full: Path,
    rel_path: str,
    size: int,
    mtime_ns: int,
    spec: SampleSpec,
    pm25_column: str = "pm25",
    *,
    expected_row_count: int | None = None,
) -> tuple[FileBatch, ...]:
    """实读一个文件，返回两类各自的候选（每类最多 ``quota`` 个）。

    单遍流式读取：对每行按其抽签键维护“当前最小的 quota 个”小顶堆，
    因此内存占用与文件行数无关，且结果与“全读后排序取前 quota 个”一致。
    """
    import heapq

    quotas = spec.quotas
    seeds = spec.seeds
    # 每类维护“当前抽签键最小的 quota 个”最大堆（元素为 -key 整数）
    heaps: dict[str, list[tuple[int, int, float | None]]] = {k: [] for k in KINDS}
    eligible = {k: 0 for k in KINDS}
    row_count = 0

    try:
        st_before = full.stat()
        if (st_before.st_size, st_before.st_mtime_ns) != (size, mtime_ns):
            # 校验与实读之间文件被替换：抽签键用的是清单定位信息，绝不能读错内容
            raise SampleError(
                f"文件在校验后被替换，已中止：{rel_path}",
                [REASON_FILE_MISMATCH],
                [f"{rel_path}: 清单 size={size}/mtime_ns={mtime_ns}，"
                 f"读取前实际 size={st_before.st_size}/mtime_ns={st_before.st_mtime_ns}"],
            )
        pf = pq.ParquetFile(full)
        if pm25_column not in pf.schema_arrow.names:
            raise SampleError(
                f"文件 {rel_path} 缺少列 {pm25_column!r}，无法抽样",
                [REASON_FILE_UNREADABLE],
                [f"{rel_path}: 实际列 {pf.schema_arrow.names}"],
            )
        for batch in pf.iter_batches(columns=[pm25_column], batch_size=65536):
            values = batch.column(0).to_pylist()
            for value in values:
                row_count += 1
                if value is not None:
                    value = float(value)
                kind = classify(value)
                if kind is None:
                    continue
                eligible[kind] += 1
                quota = quotas[kind]
                if quota <= 0:
                    continue
                digest = hmac.new(
                    seeds[kind].encode("utf-8"),
                    f"{rel_path}\x00{size}\x00{mtime_ns}\x00{row_count}".encode("utf-8"),
                    hashlib.sha256,
                ).digest()
                key_int = int.from_bytes(digest, "big")
                entry = (-key_int, row_count, value)
                heap = heaps[kind]
                if len(heap) < quota:
                    heapq.heappush(heap, entry)
                elif key_int < -heap[0][0]:
                    heapq.heapreplace(heap, entry)
        st_after = full.stat()
        if (st_before.st_size, st_before.st_mtime_ns) != (
            st_after.st_size, st_after.st_mtime_ns
        ):
            raise SampleError(
                f"文件在读取过程中被替换，已中止：{rel_path}",
                [REASON_FILE_MISMATCH],
                [f"{rel_path}: 读取前后 size/mtime 不一致，无法保证与清单对应"],
            )
    except SampleError:
        raise
    except Exception as exc:  # PyArrow 对损坏文件抛出多种异常：统一转成读取失败
        raise SampleError(
            f"读取文件失败，已中止：{rel_path}",
            [REASON_FILE_UNREADABLE],
            [f"{rel_path}: {exc.__class__.__name__}: {exc}"],
        ) from exc

    if expected_row_count is not None and expected_row_count != row_count:
        raise SampleError(
            f"文件 {rel_path} 实际行数 {row_count} 与 footer 声明 {expected_row_count} 不一致",
            [REASON_FILE_UNREADABLE],
        )

    batches: list[FileBatch] = []
    for kind in KINDS:
        # 堆里是抽签键最小的 quota 个；按抽签键升序即为抽签顺序
        selected = sorted(heaps[kind], key=lambda item: -item[0])
        candidates = tuple(
            Candidate(
                kind=kind,
                station=spec.station,
                month=spec.month,
                rel_path=rel_path,
                size=size,
                mtime_ns=mtime_ns,
                row_number=row_no,
                value=value,
                draw_key=format(-neg_key, "064x"),
            )
            for neg_key, row_no, value in selected
        )
        batches.append(FileBatch(
            rel_path=rel_path, size=size, mtime_ns=mtime_ns, kind=kind,
            row_count=row_count, eligible_rows=eligible[kind],
            candidates=candidates,
        ))
    return tuple(batches)


# --------------------------------------------------------------------------- #
# 范围、排序与合并
# --------------------------------------------------------------------------- #
def month_bounds(month: str) -> tuple[date, date]:
    try:
        year_s, month_s = month.split("-")
        year, mon = int(year_s), int(month_s)
        first = date(year, mon, 1)
    except (ValueError, AttributeError):
        raise SampleError(
            f"月份格式非法，应为 YYYY-MM: {month!r}", [REASON_INVALID_SPEC]
        ) from None
    nxt = date(year + 1, 1, 1) if mon == 12 else date(year, mon + 1, 1)
    return first, nxt


def select_files(
    snapshot: Snapshot, station: str, month: str
) -> list[tuple[FileMeta, tuple[int, int]]]:
    """按站点的日期分区筛选月份范围内的文件。"""
    first, nxt = month_bounds(month)
    picked: list[tuple[FileMeta, tuple[int, int]]] = []
    for meta in snapshot.files:
        if meta.station != station or meta.date is None:
            continue
        try:
            day = date.fromisoformat(meta.date)
        except ValueError:
            continue
        if not (first <= day < nxt):
            continue
        declared = snapshot.declared_for(meta.rel_path)
        if declared is None:
            continue
        picked.append((meta, declared))
    picked.sort(key=lambda item: item[0].rel_path)
    return picked


def _merge(candidates: dict[str, list[Candidate]], quota: int) -> list[Candidate]:
    merged = [c for items in candidates.values() for c in items]
    merged.sort(key=lambda c: (c.draw_key, c.rel_path, c.row_number))
    return merged[:quota]


def _eligible_note(kind: str, quota: int, eligible: int, notes: list[str]) -> None:
    label = "超标" if kind == KIND_EXCEED else "缺测"
    if eligible == 0:
        notes.append(f"{label}类：范围内没有合格记录，名单为空")
    elif eligible < quota:
        notes.append(f"{label}类：合格记录仅 {eligible} 条，不足名额 {quota}，已全部取用")


def build_spec(
    *, station: str, month: str, exceed_quota: int, missing_quota: int,
    exceed_seed: str, missing_seed: str | None,
) -> SampleSpec:
    station = (station or "").strip()
    month = (month or "").strip()
    exceed_seed = (exceed_seed or "").strip()
    missing_seed = (missing_seed or "").strip() or exceed_seed
    if not station:
        raise SampleError("站点不能为空", [REASON_INVALID_SPEC])
    if not exceed_seed:
        raise SampleError("抽样种子不能为空", [REASON_INVALID_SPEC])
    month_bounds(month)
    if exceed_quota < 0 or missing_quota < 0:
        raise SampleError("名额不能为负数", [REASON_INVALID_SPEC])
    if exceed_quota > 100_000 or missing_quota > 100_000:
        raise SampleError("名额过大（单类上限 100000）", [REASON_INVALID_SPEC])
    return SampleSpec(
        station=station, month=month,
        exceed_quota=exceed_quota, missing_quota=missing_quota,
        exceed_seed=exceed_seed, missing_seed=missing_seed,
    )


def snapshot_summary(snapshot: Snapshot, in_range: list[FileMeta]) -> dict:
    """落盘保存的快照证据：ID、清单版本、范围内逐文件定位信息。"""
    return {
        "snapshot_id": snapshot.snapshot_id,
        "source": snapshot.source,
        "generated_at": snapshot.generated_at,
        "archive_dir": str(snapshot.archive_dir),
        "manifest_path": str(snapshot.manifest_path) if snapshot.manifest_path else None,
        "manifest_version": snapshot.manifest_version,
        "manifest_size": snapshot.manifest_size,
        "manifest_mtime_ns": snapshot.manifest_mtime_ns,
        "file_count": len(snapshot.files),
        "total_bytes": sum(f.size for f in snapshot.files),
        "files_in_range": [
            {
                "path": m.rel_path,
                "size": snapshot.declared_for(m.rel_path)[0],
                # 与 API 一致：超出 JS 安全整数范围，按十进制字符串保存
                "mtime_ns": str(snapshot.declared_for(m.rel_path)[1]),
                "date": m.date,
                "row_count": m.row_count,
                "stats_status": m.stats_status,
            }
            for m in in_range
        ],
    }


def _validate_files(
    files: list[tuple[FileMeta, tuple[int, int]]], root: Path
) -> None:
    problems = []
    codes = set()
    for meta, declared in files:
        status, error = check_file(meta, declared, root)
        if status == "ok":
            continue
        problems.append(f"{meta.rel_path}: {error}")
        codes.add(
            REASON_FILE_MISMATCH if status == "mismatch"
            else REASON_FILE_MISSING if status == "missing"
            else REASON_FILE_UNREADABLE
        )
    if problems:
        raise SampleError(
            "以下清单文件与磁盘不符、缺失或不可读，已中止并保留上次成功名单：",
            sorted(codes),
            problems,
        )


# --------------------------------------------------------------------------- #
# 生成 / 更新
# --------------------------------------------------------------------------- #
def build_full(snapshot: Snapshot, spec: SampleSpec, pm25_column: str = "pm25") -> SampleList:
    """按当前清单对范围内全部文件实读抽签（全量基准）。"""
    files = select_files(snapshot, spec.station, spec.month)
    if not files:
        raise SampleError(
            f"站点 {spec.station!r} 在 {spec.month} 没有清单文件，无法抽样",
            [REASON_NO_FILES],
            ["请确认站点/月份，或清单是否已包含该月文件"],
        )
    _validate_files(files, snapshot.archive_dir)

    notes = [
        f"口径：PM2.5 ≥ {EXCEED_THRESHOLD:g} μg/m³ 记超标；null 记缺测；"
        "两类各自等概率无放回抽取",
        f"站点 {spec.station} · 月份 {spec.month} · "
        f"超标种子 {spec.exceed_seed!r} · 缺测种子 {spec.missing_seed!r}",
    ]
    batches: dict[str, tuple[FileBatch, ...]] = {}
    for meta, (size, mtime_ns) in files:
        batches[meta.rel_path] = scan_file(
            snapshot.archive_dir / meta.rel_path, meta.rel_path, size, mtime_ns,
            spec, pm25_column, expected_row_count=meta.row_count,
        )

    return _finish(
        snapshot, spec, batches, notes,
        source="generate", previous_snapshot_id=None,
    )


def _base_notes(spec: SampleSpec) -> list[str]:
    return [
        f"口径：PM2.5 ≥ {EXCEED_THRESHOLD:g} μg/m³ 记超标；null 记缺测；"
        "两类各自等概率无放回抽取",
        f"站点 {spec.station} · 月份 {spec.month} · "
        f"超标种子 {spec.exceed_seed!r} · 缺测种子 {spec.missing_seed!r}",
    ]


def _finish(
    snapshot: Snapshot,
    spec: SampleSpec,
    batches: dict[str, tuple[FileBatch, ...]],
    notes: list[str],
    *,
    source: Literal["generate", "update", "update_noop"],
    previous_snapshot_id: str | None,
) -> SampleList:
    # 每次生成都从固定口径说明 + 本次过程说明重建，避免多次更新后 notes 累积
    notes = _base_notes(spec) + notes
    final: dict[str, list[Candidate]] = {}
    eligible: dict[str, int] = {}
    for kind in KINDS:
        quota = spec.quotas[kind]
        per_file = {
            rel: next(b for b in items if b.kind == kind).candidates
            for rel, items in batches.items()
        }
        eligible[kind] = sum(
            next(b for b in items if b.kind == kind).eligible_rows
            for items in batches.values()
        )
        final[kind] = _merge(per_file, quota)
        _eligible_note(kind, quota, eligible[kind], notes)
    in_range = [snapshot.by_relpath()[rel] for rel in sorted(batches)
                if rel in snapshot.by_relpath()]
    return SampleList(
        spec=spec,
        snapshot_id=snapshot.snapshot_id,
        snapshot=snapshot_summary(snapshot, in_range),
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        source=source,
        batches=batches,
        candidates=final,
        eligible_counts=eligible,
        notes=notes,
        previous_snapshot_id=previous_snapshot_id,
    )


@dataclass
class UpdateResult:
    sample: SampleList
    added: dict[str, list[Candidate]]
    withdrawn: dict[str, list[Candidate]]
    reused_files: list[str]
    read_files: list[str]
    unchanged_snapshot: bool

    def to_json(self) -> dict:
        return {
            "added": {k: [c.to_json() for c in v] for k, v in self.added.items()},
            "withdrawn": {k: [c.to_json() for c in v] for k, v in self.withdrawn.items()},
            "reused_files": self.reused_files,
            "read_files": self.read_files,
            "unchanged_snapshot": self.unchanged_snapshot,
        }


def _diff(old: SampleList, new: SampleList) -> tuple[
    dict[str, list[Candidate]], dict[str, list[Candidate]]
]:
    added: dict[str, list[Candidate]] = {}
    withdrawn: dict[str, list[Candidate]] = {}
    for kind in KINDS:
        old_ids = {c.locator for c in old.candidates[kind]}
        new_ids = {c.locator for c in new.candidates[kind]}
        added[kind] = [c for c in new.candidates[kind] if c.locator not in old_ids]
        withdrawn[kind] = [c for c in old.candidates[kind] if c.locator not in new_ids]
    return added, withdrawn


def diff_samples(old: SampleList, new: SampleList) -> dict:
    """标出相对上次成功名单的新增/撤回样本（含定位信息）。"""
    added, withdrawn = _diff(old, new)
    return {
        "added": {k: [c.to_json() for c in v] for k, v in added.items()},
        "withdrawn": {k: [c.to_json() for c in v] for k, v in withdrawn.items()},
        "reused_files": [],
        "read_files": sorted(new.batches),
        "unchanged_snapshot": old.snapshot_id == new.snapshot_id,
    }


def build_update(
    snapshot: Snapshot,
    previous: SampleList,
    spec: SampleSpec,
    *,
    pm25_column: str = "pm25",
    max_incremental_files: int = 256,
) -> UpdateResult:
    """按原条件更新：只实读新增/换版文件，结果与全量抽样逐条一致。"""
    if previous.spec != spec:
        raise SampleError(
            "更新参数与上次成功名单不一致（站点/月份/名额/种子必须相同），"
            "请改用“重新生成”",
            [REASON_SPEC_CHANGED],
            [f"上次: {previous.spec.to_json()}", f"本次: {spec.to_json()}"],
        )

    files = select_files(snapshot, spec.station, spec.month)
    if not files:
        raise SampleError(
            f"站点 {spec.station!r} 在 {spec.month} 已没有清单文件（可能被撤回），"
            "无法更新；上次成功名单保持不变",
            [REASON_NO_FILES],
        )
    _validate_files(files, snapshot.archive_dir)

    previous_by_identity: dict[str, tuple[FileBatch, ...]] = {}
    for rel, items in previous.batches.items():
        by_kind = {b.kind: b for b in items}
        if set(by_kind) != set(KINDS):
            raise SampleError(
                f"上次成功名单中文件 {rel} 的证据不完整，无法增量复用",
                [REASON_STATE_CORRUPT],
            )
        previous_by_identity[file_identity(rel, items[0].size, items[0].mtime_ns)] = items

    changed = [
        (meta, declared)
        for meta, declared in files
        if file_identity(meta.rel_path, *declared) not in previous_by_identity
    ]
    if len(changed) > max_incremental_files:
        raise SampleError(
            f"本次新增/换版文件 {len(changed)} 个，超过单次增量实读上限 "
            f"{max_incremental_files}；请调高上限或改用“重新生成”",
            [REASON_LIMIT],
        )

    notes: list[str] = []
    read_files = [meta.rel_path for meta, _ in changed]
    read_set = set(read_files)
    reused_files = [meta.rel_path for meta, _ in files if meta.rel_path not in read_set]
    unchanged_snapshot = previous.snapshot_id == snapshot.snapshot_id
    if unchanged_snapshot:
        notes.append("清单条目与上次成功名单相同：未实读任何文件，名单保持不变")
    else:
        notes.append(
            f"本次只实读 {len(read_files)} 个新增/换版文件；"
            f"复用 {len(reused_files)} 个未变文件的抽签证据"
        )

    batches: dict[str, tuple[FileBatch, ...]] = {}
    for meta, declared in files:
        identity = file_identity(meta.rel_path, *declared)
        prior = previous_by_identity.get(identity)
        if prior is not None:
            batches[meta.rel_path] = prior
            continue
        batches[meta.rel_path] = scan_file(
            snapshot.archive_dir / meta.rel_path, meta.rel_path,
            declared[0], declared[1], spec, pm25_column,
            expected_row_count=meta.row_count,
        )

    sample = _finish(
        snapshot, spec, batches, notes,
        source="update_noop" if unchanged_snapshot else "update",
        previous_snapshot_id=previous.snapshot_id,
    )
    added, withdrawn = _diff(previous, sample)
    return UpdateResult(
        sample=sample, added=added, withdrawn=withdrawn,
        reused_files=reused_files, read_files=read_files,
        unchanged_snapshot=unchanged_snapshot,
    )


def compare_with_full(
    snapshot: Snapshot, sample: SampleList, pm25_column: str = "pm25"
) -> dict:
    """审计：按当前清单全量抽样，与给定名单逐条比对（会读范围内全部文件）。"""
    reference = build_full(snapshot, sample.spec, pm25_column)
    differences: list[str] = []
    details: dict[str, dict] = {}
    for kind in KINDS:
        a = {c.locator: c for c in sample.candidates[kind]}
        b = {c.locator: c for c in reference.candidates[kind]}
        missing = sorted(set(b) - set(a))
        extra = sorted(set(a) - set(b))
        key_mismatch = sorted(
            loc for loc in set(a) & set(b)
            if a[loc].draw_key != b[loc].draw_key
        )
        details[kind] = {
            "missing": len(missing), "extra": len(extra),
            "key_mismatch": len(key_mismatch),
        }
        if missing:
            differences.append(f"{kind}: 缺少 {len(missing)} 条全量样本")
        if extra:
            differences.append(f"{kind}: 多出 {len(extra)} 条不在全量样本中")
        if key_mismatch:
            differences.append(f"{kind}: {len(key_mismatch)} 条抽签键不一致")
    return {
        "equivalent": not differences,
        "differences": differences,
        "details": details,
        "full_snapshot_id": snapshot.snapshot_id,
        "sample_snapshot_id": sample.snapshot_id,
    }


# --------------------------------------------------------------------------- #
# 持久化：上次成功名单 + 对应快照 + 最近一次尝试
# --------------------------------------------------------------------------- #
def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _read_json(path: Path) -> dict | None:
    try:
        with path.open("rb") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        raise SampleError(
            f"已保存的状态文件损坏: {path}（{exc.msg}）", [REASON_STATE_CORRUPT]
        ) from None


class SampleStore:
    """把每个（站点, 月份）的上次成功名单原子落盘，重启后仍可查看。"""

    def __init__(self, state_dir: Path) -> None:
        self._dir = state_dir

    @staticmethod
    def key_for(station: str, month: str) -> str:
        return f"{station}__{month}"

    def _key_dir(self, station: str, month: str) -> Path:
        key = self.key_for(station, month)
        safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in key)
        return self._dir / safe

    def load(self, station: str, month: str) -> SampleList | None:
        payload = _read_json(self._key_dir(station, month) / "last_success.json")
        if payload is None:
            return None
        try:
            return SampleList.from_json(payload)
        except SampleError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise SampleError(
                f"已保存的名单无法解析（{exc.__class__.__name__}: {exc}）",
                [REASON_STATE_CORRUPT],
            ) from None

    def save(self, sample: SampleList) -> None:
        _atomic_write_json(
            self._key_dir(sample.spec.station, sample.spec.month) / "last_success.json",
            sample.to_json(),
        )

    def load_diff(self, station: str, month: str) -> dict | None:
        return _read_json(self._key_dir(station, month) / "last_diff.json")

    def save_diff(self, station: str, month: str, diff: dict) -> None:
        _atomic_write_json(
            self._key_dir(station, month) / "last_diff.json", diff
        )

    def load_attempt(self, station: str, month: str) -> dict | None:
        return _read_json(self._key_dir(station, month) / "last_attempt.json")

    def save_attempt(self, station: str, month: str, payload: dict) -> None:
        _atomic_write_json(
            self._key_dir(station, month) / "last_attempt.json", payload
        )

    def list_saved(self) -> list[dict]:
        out: list[dict] = []
        if not self._dir.exists():
            return out
        for child in sorted(self._dir.iterdir()):
            path = child / "last_success.json"
            if not path.is_file():
                continue
            try:
                payload = _read_json(path) or {}
                spec = payload.get("spec", {})
                out.append({
                    "station": spec.get("station"),
                    "month": spec.get("month"),
                    "snapshot_id": payload.get("snapshot_id"),
                    "generated_at": payload.get("generated_at"),
                    "counts": {
                        k: len(payload.get("candidates", {}).get(k, [])) for k in KINDS
                    },
                })
            except SampleError as exc:
                out.append({"path": str(path), "error": str(exc)})
        return out
