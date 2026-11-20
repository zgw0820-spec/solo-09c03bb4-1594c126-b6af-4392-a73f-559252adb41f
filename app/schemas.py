"""Pydantic 模型 —— 字段描述同时生成 OpenAPI/Swagger 文档。"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from .config import settings


class TableRef(BaseModel):
    schema_name: str = Field(default="public", description="目标表所在 schema")
    table_name: str = Field(..., description="目标表名（如 device_events）")


class SnapshotSession(BaseModel):
    pid: int
    role_name: str | None = None
    application_name: str | None = None
    client_addr: str | None = None
    state: str | None = None
    backend_start: datetime
    xact_start: datetime | None = None
    state_change: datetime | None = None
    xact_age: timedelta | None = Field(
        None, description="事务已持续时长（序列化为秒）"
    )
    backend_age: timedelta | None = None
    backend_xid: str | None = None
    backend_xmin: str | None = None
    current_query: str | None = None


class TableStats(BaseModel):
    db_name: str
    table_schema: str
    table_name: str
    relation: str
    n_dead_tup_estimate: int = Field(
        ..., description="死元组【估算】值，来自规划器统计，非精确计数"
    )
    n_live_tup_estimate: int = Field(..., description="活元组【估算】值")
    dead_tuple_ratio_estimate: float | None = Field(
        None, description="死元组占比【估算】，统计未刷新时可能为 null"
    )
    vacuum_count: int
    autovacuum_count: int
    last_vacuum: datetime | None = None
    last_autovacuum: datetime | None = None
    last_analyze: datetime | None = None
    last_autoanalyze: datetime | None = None
    relation_size_bytes: int = Field(
        ..., description="表（不含索引）当前占用字节；非本次清理释放量"
    )
    total_relation_size_bytes: int = Field(
        ..., description="表+索引+TOAST 当前占用字节；非本次清理释放量"
    )


class PreparedXact(BaseModel):
    gid: str
    owner: str
    database: str
    prepared: datetime
    age: timedelta | None = None


class ReplicationSlotInfo(BaseModel):
    slot_name: str
    slot_type: str
    active: bool
    xmin: str | None = None
    catalog_xmin: str | None = None
    retained_wal_bytes: int | None = None


class DiagnosticsResponse(BaseModel):
    generated_at: datetime
    table: TableStats
    blocking_sessions: list[SnapshotSession] = Field(
        ...,
        description="持有旧快照、可能阻碍死元组回收的会话；"
        "终止操作必须回传其中的 pid/backend_start/xact_start",
    )
    prepared_transactions: list[PreparedXact]
    inactive_replication_slots: list[ReplicationSlotInfo]
    estimates_notice: str = Field(
        "n_dead_tup_estimate 等为规划器估算值，不是精确死元组计数，"
        "更不代表可释放的磁盘字节；普通 VACUUM 通常不向操作系统归还磁盘空间。"
    )


class TerminateRequest(BaseModel):
    pid: int = Field(..., description="诊断结果中给出的 PID")
    backend_start: datetime = Field(
        ..., description="诊断结果中给出的 backend_start，必须原样回传"
    )
    xact_start: datetime = Field(
        ..., description="诊断结果中给出的事务起点 xact_start，必须原样回传"
    )
    schema_name: str = Field(default="public")
    table_name: str = Field(..., description="目标表名，用于审计关联")


class TerminateResponse(BaseModel):
    terminated: bool
    decision: Literal["terminated", "rejected"]
    target_pid: int
    reason: str
    observed_session: dict[str, Any] | None = Field(
        None, description="再校验时观察到的会话实际状态（审计留痕）"
    )


class VacuumRequest(BaseModel):
    schema_name: str = Field(default="public")
    table_name: str
    wait: bool = Field(
        False,
        description="true 时同步等待本次 VACUUM 结束（受 SYNC_WAIT_SECONDS 限制）；"
        "false 立即返回作业 id，凭作业接口轮询",
    )


class StatsSnapshot(BaseModel):
    n_dead_tup_estimate: int
    n_live_tup_estimate: int
    vacuum_count: int
    autovacuum_count: int
    last_vacuum: datetime | None = None
    last_autovacuum: datetime | None = None
    relation_size_bytes: int
    total_relation_size_bytes: int


class JobResponse(BaseModel):
    id: int
    table_schema: str
    table_name: str
    status: Literal["running", "succeeded", "failed", "interrupted"]
    started_at: datetime
    finished_at: datetime | None = None
    statement_timeout_ms: int
    before_snapshot: dict[str, Any]
    result: dict[str, Any] | None = None
    error_message: str | None = None
    verification_hint: str | None = Field(
        None,
        description="interrupted 作业的核实结论: 重启后对比统计基线判断 VACUUM 是否实际完成",
    )
    disk_notice: str | None = Field(
        None,
        description="relation_size_bytes 为当前占用，差值不等于'已释放磁盘字节'",
    )


class HealthResponse(BaseModel):
    status: Literal["ok", "error"]
    database: str | None = None
    detail: str | None = None


# --- 表级 autovacuum 插入阈值临时覆盖 ----------------------------------------

OverrideStatus = Literal["active", "pending_restore", "restored", "preserved"]


class AutovacuumOverrideRequest(BaseModel):
    """申请临时调整某表的按插入量触发 autovacuum 门槛。"""

    request_no: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="申请号；同一表内唯一。原样重送（参数、时长一致）"
        "会返回原记录与原截止时间，不会新开窗口",
    )
    schema_name: str = Field(default="public", description="目标表所在 schema")
    table_name: str = Field(..., description="目标表名（如 device_events，须为普通表）")
    autovacuum_vacuum_insert_scale_factor: float = Field(
        ...,
        description=(
            "申请生效的表级 autovacuum_vacuum_insert_scale_factor。"
            "含义: 插入量达到 autovacuum_vacuum_insert_threshold + "
            "此系数 × 表大小 时触发 autovacuum。允许 0~100，"
            "与 PostgreSQL 16 reloption 范围一致"
        ),
    )
    duration_seconds: int = Field(
        ...,
        description="生效时长（秒）；到期后自动恢复申请前的表级设定，"
        "原为继承则恢复继承。范围 1 秒 ~ OVERRIDE_MAX_DURATION_HOURS",
    )

    @field_validator("request_no")
    @classmethod
    def _strip_request_no(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("request_no 不能为空")
        return v

    @field_validator("table_name")
    @classmethod
    def _strip_table_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("table_name 不能为空")
        return v

    @field_validator("autovacuum_vacuum_insert_scale_factor")
    @classmethod
    def _check_scale(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("scale factor 必须是有限数值")
        if not (
            settings.override_scale_factor_min
            <= v
            <= settings.override_scale_factor_max
        ):
            raise ValueError(
                "scale factor 必须在 "
                f"{settings.override_scale_factor_min:g} ~ "
                f"{settings.override_scale_factor_max:g} 之间"
            )
        return v

    @field_validator("duration_seconds")
    @classmethod
    def _check_duration(cls, v: int) -> int:
        if v < 1:
            raise ValueError("duration_seconds 至少为 1")
        if v > settings.override_max_duration_hours * 3600:
            raise ValueError(
                "duration_seconds 超过上限 "
                f"{settings.override_max_duration_hours:g} 小时"
            )
        return v


class AutovacuumOverrideResponse(BaseModel):
    id: int
    request_no: str
    table_schema: str
    table_name: str
    requested_scale_factor: float
    requested_duration_seconds: int
    duration_seconds: int
    status: OverrideStatus
    applied_at: datetime
    expires_at: datetime
    seconds_until_expiry: float | None = Field(
        None, description="距截止时间剩余秒数；已到期为 0"
    )
    original_source: Literal["inherited", "table"] = Field(
        ..., description="申请前的来源：inherited=继承实例默认值；table=表级设定"
    )
    original_raw_value: str | None = Field(
        None, description="申请前的表级值（original_source=inherited 时为 null）"
    )
    applied_raw_value: str = Field(..., description="本申请写入的表级值")
    restore_attempts: int
    last_restore_attempt_at: datetime | None = None
    resolved_at: datetime | None = None
    outcome_detail: str | None = Field(
        None, description="终态说明；preserved 时说明检测到的人工改动"
    )
    last_error: str | None = None
    replayed: bool = Field(
        False, description="true 表示同一申请号原样重送，沿用原记录与原截止时间"
    )
    next_action: str = ""
    db_now: datetime = Field(
        ..., description="数据库当前时间，用于核对截止时间（避免客户端时钟偏差）"
    )


class AutovacuumOverrideQuery(BaseModel):
    schema_name: str = "public"
    table_name: str = "device_events"
    status: OverrideStatus | None = Field(
        None, description="按状态过滤；omit 表示全部"
    )
    limit: int = Field(50, ge=1, le=200)


class InsertScaleFactorSetting(BaseModel):
    option: str
    source: Literal["inherited", "table"] = Field(
        ..., description="库里实际来源：table=该表有表级 reloption；inherited=无表级设定"
    )
    effective_value: float = Field(
        ..., description="当前实际生效值（表级值，或继承的实例默认值）"
    )
    table_raw_value: str | None = Field(
        None, description="pg_class.reloptions 中的原始文本；null 表示无表级设定"
    )
    instance_default_value: float = Field(
        ..., description="实例级 autovacuum_vacuum_insert_scale_factor"
    )
    autovacuum_enabled_table_raw: str | None = Field(
        None, description="表级 autovacuum_enabled 原值；null 表示未设置"
    )
    autovacuum_instance_enabled: bool
    source_explanation: str


class OpenOverrideInfo(BaseModel):
    id: int
    request_no: str
    status: Literal["active", "pending_restore"]
    applied_at: datetime
    expires_at: datetime
    seconds_until_expiry: float
    note: str


class TableOverrideSettingResponse(BaseModel):
    table_schema: str
    table_name: str
    generated_at: datetime
    insert_scale_factor: InsertScaleFactorSetting
    open_override: OpenOverrideInfo | None = Field(
        None, description="该表当前未结束的申请；null 表示没有"
    )
    history: list[dict[str, Any]] = Field(
        default_factory=list, description="该表最近的申请记录（倒序）"
    )


class OverrideActionResponse(BaseModel):
    message: str
    override: AutovacuumOverrideResponse


class OverrideListResponse(BaseModel):
    overrides: list[AutovacuumOverrideResponse]
