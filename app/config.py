"""运行期配置，全部来自环境变量，均带默认值。"""
from __future__ import annotations

import os


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


class Settings:
    # 例: postgresql://user:pass@db:5432/devicedb
    database_url: str = os.environ.get(
        "DATABASE_URL", "postgresql://postgres:postgres@db:5432/devicedb"
    )
    # 维护作业元数据所在的 schema/表（API 自建，不与业务表混用）
    job_schema: str = os.environ.get("MAINT_SCHEMA", "maintenance")
    job_table: str = os.environ.get("MAINT_JOB_TABLE", "vacuum_jobs")
    audit_table: str = os.environ.get("MAINT_AUDIT_TABLE", "termination_audit")
    # 连接池容量
    pool_min_size: int = _int("POOL_MIN_SIZE", 1)
    pool_max_size: int = _int("POOL_MAX_SIZE", 10)
    # VACUUM 语句级超时（毫秒），0 表示不限制
    vacuum_statement_timeout_ms: int = _int("VACUUM_STATEMENT_TIMEOUT_MS", 0)
    # POST /vacuum?wait=true 时等待作业结束的最长秒数
    sync_wait_seconds: int = _int("SYNC_WAIT_SECONDS", 30)
    # 插入量触发 autovacuum 的临时阈值申请（maintenance.autovacuum_overrides）
    override_table: str = os.environ.get(
        "MAINT_OVERRIDE_TABLE", "autovacuum_overrides"
    )
    # 恢复任务轮询间隔与单次恢复尝试的锁等待上限（秒）
    override_reconcile_interval_seconds: float = _float(
        "OVERRIDE_RECONCILE_INTERVAL_SECONDS", 1.0
    )
    override_restore_lock_timeout_seconds: int = _int(
        "OVERRIDE_RESTORE_LOCK_TIMEOUT_SECONDS", 3
    )
    # 申请值允许区间，与 PG16 reloption 范围保持一致（autovacuum 插入阈值 0~100）
    override_scale_factor_min: float = _float("OVERRIDE_SCALE_FACTOR_MIN", 0.0)
    override_scale_factor_max: float = _float("OVERRIDE_SCALE_FACTOR_MAX", 100.0)
    # 生效时长上限（小时），防止误填超大值长期改变实例行为
    override_max_duration_hours: float = _float("OVERRIDE_MAX_DURATION_HOURS", 720.0)


settings = Settings()
