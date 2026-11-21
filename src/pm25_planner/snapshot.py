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
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings
from .parquet_meta import FileMeta, extract_file_meta

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    source: str  # "scan" | "manifest"
    files: tuple[FileMeta, ...]
    generated_at: str
    archive_dir: Path
    manifest_path: Path | None

    def by_relpath(self) -> dict[str, FileMeta]:
        return {f.rel_path: f for f in self.files}


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
                fingerprint, entries = self._read_manifest()
            else:
                fingerprint, entries = self._scan_archive()

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
            for dirpath, dirnames, filenames in __import__("os").walk(root):
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
    def _read_manifest(self) -> tuple[tuple, list[tuple[str, int, int]]]:
        mp = self._settings.manifest_path
        assert mp is not None
        st = mp.stat()  # 清单不存在会抛 FileNotFoundError，由 API 层转 503
        with mp.open("rb") as fh:
            payload = json.load(fh)
        raw_entries = payload.get("files", [])
        entries: list[tuple[str, int, int]] = []
        for item in raw_entries:
            rel = str(item["path"]).replace("\\", "/").lstrip("./")
            size = int(item.get("size", -1))
            mtime = int(item.get("mtime_ns", -1))
            entries.append((rel, size, mtime))
        entries.sort(key=lambda e: e[0])
        # 清单文件本身的 mtime/size 也纳入指纹，替换后立刻可感知
        fingerprint = ("manifest", str(mp), st.st_size, st.st_mtime_ns, tuple(entries))
        return fingerprint, entries

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
