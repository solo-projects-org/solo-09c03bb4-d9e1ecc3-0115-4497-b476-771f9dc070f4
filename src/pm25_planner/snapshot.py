"""档案快照管理。

两种来源：
- ``scan``：递归扫描档案目录（默认，适合本地样例）；
- ``manifest``：读取外部清单 JSON（清单替换期间，调用方通过
  ``os.replace`` 原子换文件，本模块永远只读到旧版或新版完整内容，
  绝不会读到半截文件）。

快照由 ``(rel_path, size, mtime_ns)`` 有序集合的 SHA-256 唯一标识；
清单一旦替换（增删文件、大小或 mtime 变化），snapshot_id 立即变化，
旧计划携带的 ID 随之失效。
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings
from .parquet_meta import FileMeta, extract_file_meta

logger = logging.getLogger(__name__)


class ManifestError(ValueError):
    """清单不可用、内容非法，或在读取过程中被并发替换。"""


@dataclass(frozen=True)
class ManifestInfo:
    """清单文件本身的版本身份与文件元数据。"""

    path: Path
    version: str | None
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    source: str  # "scan" | "manifest"
    files: tuple[FileMeta, ...]
    generated_at: str
    archive_dir: Path
    manifest_path: Path | None
    # 清单声明的 (size, mtime_ns)；扫描模式下为当时实际值
    declared: dict[str, tuple[int, int]] = field(default_factory=dict)
    manifest_version: str | None = None
    manifest_size: int | None = None
    manifest_mtime_ns: int | None = None

    def by_relpath(self) -> dict[str, FileMeta]:
        return {f.rel_path: f for f in self.files}

    def declared_for(self, rel_path: str) -> tuple[int, int] | None:
        return self.declared.get(rel_path)


def _normalize_rel_path(raw: str) -> str:
    rel = raw.replace("\\", "/").strip()
    while rel.startswith("./"):
        rel = rel[2:]
    return rel.lstrip("/")


def _is_safe_rel_path(rel: str) -> bool:
    """拒绝绝对路径、.. 穿越与空路径；清单只能指向档案根目录内部。"""
    if not rel or "\x00" in rel:
        return False
    parts = rel.split("/")
    return all(part not in ("", ".", "..") for part in parts)


def _manifest_version(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, (str, int, float, bool)):
        return str(raw)
    try:
        return json.dumps(raw, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return str(raw)


class _LRU:
    """按 (rel_path, size, mtime_ns) 失效的元数据 LRU 缓存。"""

    def __init__(self, capacity: int) -> None:
        self._cap = max(capacity, 1)
        self._data: "OrderedDict[str, tuple[tuple[int, int], FileMeta]]" = OrderedDict()
        self._lock = threading.Lock()

    def get_or_load(self, rel_path: str, path: Path, size: int, mtime_ns: int,
                    pm25_column: str) -> FileMeta:
        key = rel_path
        sig = (size, mtime_ns)
        with self._lock:
            hit = self._data.get(key)
            if hit is not None and hit[0] == sig:
                self._data.move_to_end(key)
                return hit[1]
        # 文件 IO 在锁外执行，避免慢文件阻塞其他请求
        meta = extract_file_meta(path, rel_path, pm25_column)
        with self._lock:
            self._data[key] = (sig, meta)
            self._data.move_to_end(key)
            while len(self._data) > self._cap:
                self._data.popitem(last=False)
        return meta


class SnapshotStore:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._cache = _LRU(settings.meta_cache_size)
        self._lock = threading.Lock()
        # 目录/清单指纹：未变化时直接复用上次 Snapshot
        self._dir_fingerprint: tuple | None = None
        self._last: Snapshot | None = None

    # ------------------------------------------------------------------ #
    # 公共接口
    # ------------------------------------------------------------------ #
    def current(self) -> Snapshot:
        """返回当前快照；文件系统无变化时复用上次结果（含元数据缓存）。"""
        with self._lock:
            if self._settings.manifest_path is not None:
                fingerprint, entries, manifest_info = self._read_manifest()
            else:
                fingerprint, entries = self._scan_archive()
                manifest_info = None

            if self._last is not None and fingerprint == self._dir_fingerprint:
                return self._last

            metas = tuple(
                self._cache.get_or_load(
                    rel, self._settings.archive_dir / rel, size, mtime,
                    self._settings.pm25_column,
                )
                for rel, size, mtime in entries
            )
            snapshot = Snapshot(
                snapshot_id=self._compute_id(entries),
                source="manifest" if self._settings.manifest_path is not None else "scan",
                files=metas,
                generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                archive_dir=self._settings.archive_dir,
                manifest_path=self._settings.manifest_path,
                declared={rel: (size, mtime) for rel, size, mtime in entries},
                manifest_version=manifest_info.version if manifest_info else None,
                manifest_size=manifest_info.size if manifest_info else None,
                manifest_mtime_ns=manifest_info.mtime_ns if manifest_info else None,
            )
            self._dir_fingerprint = fingerprint
            self._last = snapshot
            return snapshot

    def invalidate(self) -> None:
        """强制下次调用重新扫描（测试或清单替换事件可主动触发）。"""
        with self._lock:
            self._dir_fingerprint = None
            self._last = None

    # ------------------------------------------------------------------ #
    # 扫描模式
    # ------------------------------------------------------------------ #
    def _scan_archive(self) -> tuple[tuple, list[tuple[str, int, int]]]:
        root = self._settings.archive_dir
        entries: list[tuple[str, int, int]] = []
        dir_mtimes: list[tuple[str, int]] = []
        if root.exists():
            for dirpath, dirnames, filenames in os.walk(root):
                dirnames.sort()
                dir_mtimes.append(
                    (str(Path(dirpath).relative_to(root)),
                     Path(dirpath).stat().st_mtime_ns)
                )
                for name in sorted(filenames):
                    if not name.lower().endswith(".parquet"):
                        continue
                    full = Path(dirpath) / name
                    try:
                        st = full.stat()
                    except OSError:
                        # 竞态：扫描时文件被删/换。用 0 占位，元数据层判 unreadable 保守保留
                        entries.append((str(full.relative_to(root)), 0, 0))
                        continue
                    entries.append(
                        (str(full.relative_to(root)), st.st_size, st.st_mtime_ns)
                    )
        entries.sort(key=lambda e: e[0])
        fingerprint = ("scan", root, tuple(sorted(dir_mtimes)), tuple(entries))
        return fingerprint, entries

    # ------------------------------------------------------------------ #
    # 清单模式
    # ------------------------------------------------------------------ #
    def _read_manifest(self) -> tuple[tuple, list[tuple[str, int, int]], ManifestInfo]:
        """读取并严格校验清单；读取期间被替换则拒绝本次结果。

        清单由外部系统以“写临时文件 + os.replace”原子替换。这里额外比较
        打开前后的 stat，若在读取窗口内发生变化（或内容与 stat 不匹配），
        视为并发替换并报错，绝不把两个版本的条目混在一起用。
        """
        mp = self._settings.manifest_path
        assert mp is not None
        st_before = mp.stat()  # 清单不存在会抛 FileNotFoundError，由 API 层转 503
        with mp.open("rb") as fh:
            raw_bytes = fh.read()
        st_after = mp.stat()
        if (st_before.st_size, st_before.st_mtime_ns) != (
            st_after.st_size, st_after.st_mtime_ns
        ):
            raise ManifestError(
                "清单在读取过程中被替换（读取前 "
                f"size={st_before.st_size}/mtime_ns={st_before.st_mtime_ns}，读取后 "
                f"size={st_after.st_size}/mtime_ns={st_after.st_mtime_ns}），"
                "请重试；本次不会产生混合版本的结果"
            )

        try:
            payload = json.loads(raw_bytes)
        except json.JSONDecodeError:
            raise
        if not isinstance(payload, dict):
            raise ManifestError("清单根节点必须是 JSON 对象")
        raw_entries = payload.get("files")
        if raw_entries is None:
            raise ManifestError("清单缺少 files 数组")
        if not isinstance(raw_entries, list):
            raise ManifestError("清单 files 字段必须是数组")

        entries: list[tuple[str, int, int]] = []
        seen: set[str] = set()
        for idx, item in enumerate(raw_entries):
            if not isinstance(item, dict):
                raise ManifestError(f"清单第 {idx} 条不是对象")
            raw_path = item.get("path")
            if not isinstance(raw_path, str) or not raw_path.strip():
                raise ManifestError(f"清单第 {idx} 条缺少合法的 path")
            rel = _normalize_rel_path(raw_path)
            if not _is_safe_rel_path(rel):
                raise ManifestError(f"清单第 {idx} 条路径非法（禁止绝对路径或 .. 穿越）: {raw_path!r}")
            if rel in seen:
                raise ManifestError(f"清单存在重复路径: {rel}")
            seen.add(rel)
            try:
                size = int(item.get("size", -1))
                mtime = int(item.get("mtime_ns", -1))
            except (TypeError, ValueError):
                raise ManifestError(f"清单第 {idx} 条 size/mtime_ns 不是整数: {rel}") from None
            if size < 0 or mtime < 0:
                raise ManifestError(f"清单第 {idx} 条 size/mtime_ns 缺失或为负: {rel}")
            entries.append((rel, size, mtime))
        entries.sort(key=lambda e: e[0])

        info = ManifestInfo(
            path=mp,
            version=_manifest_version(payload.get("version")),
            size=st_after.st_size,
            mtime_ns=st_after.st_mtime_ns,
        )
        # 清单文件本身的大小/mtime 与版本号一并纳入指纹，替换后立刻可感知
        fingerprint = (
            "manifest", str(mp), info.size, info.mtime_ns,
            info.version, tuple(entries),
        )
        return fingerprint, entries, info

    @staticmethod
    def _compute_id(entries: list[tuple[str, int, int]]) -> str:
        h = hashlib.sha256()
        for rel, size, mtime in entries:
            h.update(rel.encode("utf-8"))
            h.update(b"\x00")
            h.update(str(size).encode("ascii"))
            h.update(b"\x00")
            h.update(str(mtime).encode("ascii"))
            h.update(b"\n")
        return h.hexdigest()[:16]
