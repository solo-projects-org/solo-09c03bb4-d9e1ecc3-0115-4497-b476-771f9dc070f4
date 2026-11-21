"""API 请求/响应数据模型。"""
from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class PlanRequest(BaseModel):
    """读取计划查询条件。所有筛选均为可选；缺省表示不限制。"""

    stations: list[str] | None = Field(
        default=None, description="站点白名单；为空或 None 表示全部站点"
    )
    date_from: date | None = Field(default=None, description="起始日期（含），ISO 格式")
    date_to: date | None = Field(default=None, description="结束日期（含），ISO 格式")
    pm25_min: float | None = Field(default=None, description="PM2.5 区间下界（含，μg/m³）")
    pm25_max: float | None = Field(default=None, description="PM2.5 区间上界（含，μg/m³）")
    include_nulls: bool = Field(
        default=True, description="计划是否覆盖缺测（null）记录；False 时仅保留确定含匹配值的文件"
    )
    # 调用 /api/plan 时传入上次拿到的 snapshot_id：服务端清单已替换则直接判定失效
    expected_snapshot_id: str | None = Field(
        default=None, description="绑定的快照 ID；与当前快照不一致时返回 409 STALE_SNAPSHOT"
    )

    @field_validator("stations")
    @classmethod
    def _strip_stations(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return None
        cleaned = [s.strip() for s in v if s and s.strip()]
        return cleaned or None

    @field_validator("pm25_min", "pm25_max")
    @classmethod
    def _reject_nan(cls, v: float | None) -> float | None:
        if v is not None and (v != v or v in (float("inf"), float("-inf"))):
            raise ValueError("PM2.5 区间边界必须是有限数值")
        return v

    def model_post_init(self, __context) -> None:  # type: ignore[override]
        if self.pm25_min is not None and self.pm25_max is not None:
            if self.pm25_min > self.pm25_max:
                raise ValueError("pm25_min 不能大于 pm25_max")
        if self.date_from is not None and self.date_to is not None:
            if self.date_from > self.date_to:
                raise ValueError("date_from 不能晚于 date_to")


class VerifyRequest(BaseModel):
    snapshot_id: str = Field(..., min_length=1, description="待核对的快照 ID")


class FileDecision(BaseModel):
    """逐文件保留/排除结论。"""

    path: str = Field(..., description="相对档案根目录的路径（含分区目录）")
    station: str | None
    date: str | None
    size_bytes: int = Field(..., description="文件字节数（计划字节数按保留文件求和）")
    decision: Literal["KEEP", "EXCLUDE"]
    reasons: list[str] = Field(..., description="人类可读的判定原因，按判定顺序累积")
    reason_codes: list[str] = Field(..., description="机器可读原因码，便于自动化核对")
    # 分区与统计信息原样回显，方便人工核对
    pm25_min: float | None = None
    pm25_max: float | None = None
    null_count: int | None = None
    row_count: int | None = None
    stats_status: Literal["ok", "no_stats", "unreadable", "missing_column"]
    exists: bool


class PlanResponse(BaseModel):
    snapshot_id: str
    snapshot_source: Literal["scan", "manifest"]
    generated_at: str
    request: PlanRequest
    files: list[FileDecision]
    kept_files: list[str]
    kept_count: int
    excluded_count: int
    total_files: int
    planned_bytes: int = Field(..., description="保留文件字节数之和（不含被排除文件）")
    available_stations: list[str]
    available_dates: list[str]
    notes: list[str]


class SnapshotResponse(BaseModel):
    snapshot_id: str
    source: Literal["scan", "manifest"]
    file_count: int
    total_bytes: int
    archive_dir: str
    manifest_path: str | None
    generated_at: str


class VerifyResponse(BaseModel):
    valid: bool
    current_snapshot_id: str
    message: str


class PreviewRow(BaseModel):
    station: str | None
    date: str | None
    source_file: str
    pm25: float | None


class PreviewResponse(BaseModel):
    snapshot_id: str
    valid_snapshot: bool
    row_count: int
    files_read: int
    rows: list[PreviewRow]
    notes: list[str]
