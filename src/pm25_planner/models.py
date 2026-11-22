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


# --------------------------------------------------------------------------- #
# 月报抽查名单
# --------------------------------------------------------------------------- #
class SampleSpecRequest(BaseModel):
    """生成名单的抽样参数。"""

    station: str = Field(..., min_length=1, description="站点分区值，如 station-A")
    month: str = Field(..., pattern=r"^\d{4}-\d{2}$", description="抽查月份 YYYY-MM")
    exceed_quota: int = Field(..., ge=0, le=100_000, description="超标类名额")
    missing_quota: int = Field(..., ge=0, le=100_000, description="缺测类名额")
    exceed_seed: str = Field(..., min_length=1, max_length=200, description="超标类抽样种子")
    missing_seed: str | None = Field(
        default=None, max_length=200,
        description="缺测类抽样种子；留空时沿用超标类种子（两类仍各自独立抽签）",
    )


class SampleUpdateRequest(SampleSpecRequest):
    """更新名单沿用原条件；参数必须与上次成功名单一致。"""


class SampleCandidateView(BaseModel):
    kind: Literal["exceed", "missing"]
    value: float | None = Field(..., description="读数；缺测为 null")
    path: str = Field(..., description="相对档案根目录的文件路径")
    size: int = Field(..., description="清单所记文件大小（字节）")
    mtime_ns: str = Field(..., description="清单所记修改时间（纳秒，十进制字符串，避免 JS 精度丢失）")
    row_number: int = Field(..., description="文件内行号，从 1 开始")
    draw_key: str = Field(..., description="抽签键；升序即抽签顺序")
    station: str
    month: str


class SampleFileView(BaseModel):
    path: str
    size: int
    mtime_ns: str
    kind: Literal["exceed", "missing"]
    row_count: int
    eligible_rows: int
    kept_candidates: int = Field(..., description="该文件为该类保留的候选数（≤ 名额）")


class SampleSpecView(BaseModel):
    station: str
    month: str
    exceed_quota: int
    missing_quota: int
    exceed_seed: str
    missing_seed: str


class SampleListResponse(BaseModel):
    spec: SampleSpecView
    snapshot_id: str
    snapshot: dict
    generated_at: str
    source: Literal["generate", "update", "update_noop"]
    previous_snapshot_id: str | None = None
    candidates: dict[str, list[SampleCandidateView]]
    eligible_counts: dict[str, int]
    notes: list[str]
    files: list[SampleFileView]
    quotas: dict[str, int]


class SampleDiffView(BaseModel):
    added: dict[str, list[SampleCandidateView]]
    withdrawn: dict[str, list[SampleCandidateView]]
    reused_files: list[str]
    read_files: list[str]
    unchanged_snapshot: bool


class SampleGenerateResponse(BaseModel):
    sample: SampleListResponse
    diff: SampleDiffView | None = None
    message: str


class SampleLastResponse(BaseModel):
    sample: SampleListResponse
    diff: SampleDiffView | None = None
    last_attempt: dict | None = None
    snapshot_still_current: bool = Field(
        ..., description="生成该名单时的清单快照是否仍等于当前快照"
    )
    current_snapshot_id: str | None = None


class SavedSampleItem(BaseModel):
    station: str | None = None
    month: str | None = None
    snapshot_id: str | None = None
    generated_at: str | None = None
    counts: dict[str, int] = Field(default_factory=dict)


class SampleAuditResponse(BaseModel):
    equivalent: bool
    differences: list[str]
    details: dict
    full_snapshot_id: str
    sample_snapshot_id: str
    rows_read: int
    notes: list[str]


class SampleErrorResponse(BaseModel):
    """失败响应体（HTTP 409/503 的 detail）。"""

    error: str
    reason_codes: list[str]
    message: str
    details: list[str] = Field(default_factory=list)
    last_success_snapshot_id: str | None = None
    last_success_generated_at: str | None = None
    current_snapshot_id: str | None = None
