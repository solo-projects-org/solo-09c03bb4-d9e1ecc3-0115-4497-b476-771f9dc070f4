"""运行配置：通过环境变量覆盖，全部带本地开发默认值。"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    return Path(raw).expanduser().resolve() if raw else default.resolve()


@dataclass(frozen=True)
class Settings:
    # Parquet 档案根目录，内部按 station=.../date=... 分区
    archive_dir: Path
    # 清单（manifest）文件路径；为空表示直接扫描档案目录
    manifest_path: Path | None
    # PM2.5 数值列名（样例数据统一使用该名称）
    pm25_column: str
    # 预览读取接口最多允许读取的文件数，防止计划外的大查询
    preview_max_files: int
    # 单文件元数据缓存上限（按路径缓存 footer 解析结果）
    meta_cache_size: int

    @staticmethod
    def from_env() -> "Settings":
        manifest_raw = os.environ.get("PM25_MANIFEST_PATH", "").strip()
        return Settings(
            archive_dir=_env_path(
                "PM25_ARCHIVE_DIR", Path(__file__).resolve().parents[2] / "data" / "archive"
            ),
            manifest_path=Path(manifest_raw).expanduser().resolve() if manifest_raw else None,
            pm25_column=os.environ.get("PM25_COLUMN", "pm25"),
            preview_max_files=int(os.environ.get("PM25_PREVIEW_MAX_FILES", "50")),
            meta_cache_size=int(os.environ.get("PM25_META_CACHE_SIZE", "4096")),
        )
