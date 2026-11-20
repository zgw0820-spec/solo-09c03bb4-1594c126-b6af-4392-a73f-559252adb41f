"""FastAPI 入口: 诊断、受控终止会话、VACUUM 作业（单飞 + 可核实/重试）。"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Response, status
from psycopg import sql

from . import db, maintenance, overrides
from .config import settings
from .schemas import (
    AutovacuumOverrideRequest,
    AutovacuumOverrideResponse,
    DiagnosticsResponse,
    HealthResponse,
    JobResponse,
    OverrideActionResponse,
    OverrideListResponse,
    TableOverrideSettingResponse,
    TerminateRequest,
    TerminateResponse,
    VacuumRequest,
)

OVERRIDE_SWEEP_INTERVAL_MAX = 60.0


async def _override_reconciler() -> None:
    """后台到期恢复轮询。

    不依赖进程内定时器状态: 每次只看库中 ``expires_at <= now()`` 的记录，
    因此服务停机跨过期限后，启动即会恢复；恢复被锁挡住时按间隔自动重试。
    """
    while True:
        try:
            await overrides.sweep_due_overrides()
        except asyncio.CancelledError:
            raise
        except Exception:
            overrides.log.exception("autovacuum override 到期恢复轮询失败")
        await asyncio.sleep(
            max(0.2, min(settings.override_reconcile_interval_seconds,
                        OVERRIDE_SWEEP_INTERVAL_MAX))
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.open_pool()
    # 建表 + 把上次进程遗留的 running 作业标记为 interrupted
    await db.init_schema()
    # 先立即扫一遍（覆盖“停机跨过期限”的场景），再进入周期轮询
    try:
        await overrides.sweep_due_overrides()
    except Exception:
        overrides.log.exception("启动时处理到期的 autovacuum override 失败")
    sweep_task = asyncio.create_task(_override_reconciler())
    try:
        yield
    finally:
        sweep_task.cancel()
        try:
            await sweep_task
        except (asyncio.CancelledError, Exception):
            pass
        await db.close_pool()


app = FastAPI(
    title="PostgreSQL 16 设备事件表 VACUUM 维护 API",
    version="1.0.0",
    description=(
        "展示设备事件表死元组【估算】与可能阻碍回收的旧快照会话；"
        "凭诊断返回的 PID/backend_start/事务起点，仅可终止同库且仍为 "
        "idle in transaction 的原会话；随后执行 VACUUM 并回读结果。\n\n"
        "- 同表并发维护单飞（部分唯一索引 + advisory lock）。\n"
        "- 请求超时或服务重启后，未确认的清理不会被报为完成，"
        "可经作业接口核实或重试。\n"
        "- 可临时提高表级 autovacuum 插入阈值，到期自动恢复原设定/继承；"
        "参数被人工改动时保留现状，被表锁挡住时标记待恢复并自动重试。\n"
        "- 所有 n_dead_tup/尺寸字段均为估值或当前占用，"
        "不代表“已释放磁盘字节”。"
    ),
    lifespan=lifespan,
)


async def _check_relation(schema_name: str, table_name: str) -> None:
    if not await db.relation_exists(schema_name, table_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"relation {schema_name}.{table_name} does not exist",
        )


@app.get("/health", response_model=HealthResponse, tags=["meta"])
async def health() -> HealthResponse:
    try:
        pool = await db.open_pool()
        async with pool.connection() as conn:
            cur = await conn.execute("SELECT current_database() AS db")
            row = await cur.fetchone()
        return HealthResponse(status="ok", database=row["db"])
    except Exception as exc:  # 连通性失败要显式报错
        return HealthResponse(status="error", detail=str(exc))


@app.get(
    "/diagnostics",
    response_model=DiagnosticsResponse,
    tags=["diagnostics"],
    summary="死元组估算 + 可能阻碍回收的旧快照会话",
)
async def diagnostics(schema_name: str = "public",
                      table_name: str = "device_events") -> DiagnosticsResponse:
    await _check_relation(schema_name, table_name)
    stats = await db.fetch_table_stats(schema_name, table_name)
    sessions = await db.fetch_blocking_sessions()
    prepared = await db.fetch_prepared_xacts()
    slots = await db.fetch_inactive_slots()
    return DiagnosticsResponse(
        generated_at=datetime.now(timezone.utc),
        table=stats,
        blocking_sessions=sessions,
        prepared_transactions=prepared,
        inactive_replication_slots=slots,
    )


@app.post(
    "/sessions/terminate",
    response_model=TerminateResponse,
    tags=["sessions"],
    summary="终止 idle in transaction 会话（身份/状态变化即拒绝）",
)
async def terminate_session(req: TerminateRequest) -> TerminateResponse:
    await _check_relation(req.schema_name, req.table_name)
    result = await maintenance.terminate_idle_session(
        req.pid, req.backend_start, req.xact_start,
        req.schema_name, req.table_name,
    )
    return TerminateResponse(**result)


@app.post(
    "/maintenance/vacuum",
    tags=["maintenance"],
    status_code=status.HTTP_202_ACCEPTED,
    summary="对目标表执行 VACUUM (VERBOSE, ANALYZE)，同表单飞",
)
async def start_vacuum(req: VacuumRequest) -> dict:
    await _check_relation(req.schema_name, req.table_name)
    job_id, conflict = await maintenance.create_vacuum_job(
        req.schema_name, req.table_name
    )
    if job_id is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "该表已有正在执行的维护作业，拒绝重复执行",
                "running_job": conflict,
            },
        )

    async def _runner() -> None:
        await maintenance.run_vacuum_job(
            job_id, req.schema_name, req.table_name
        )

    asyncio.create_task(_runner())

    if req.wait:
        deadline = asyncio.get_event_loop().time() + settings.sync_wait_seconds
        while True:
            job = await _load_job(job_id)
            if job["status"] != "running":
                return {"job": _serialize_job(job)}
            if asyncio.get_event_loop().time() >= deadline:
                return {
                    "job": _serialize_job(job),
                    "message": "等待超时，作业仍在后台执行；"
                               "未确认完成，不会报为成功，请轮询作业接口",
                }
            await asyncio.sleep(0.25)

    return {
        "job_id": job_id,
        "status": "running",
        "message": "作业已受理；VACUUM 在后台执行，凭 job_id 轮询核实",
    }


async def _load_job(job_id: int) -> dict:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            sql.SQL("SELECT * FROM {sch}.{tbl} WHERE id = %s").format(
                sch=sql.Identifier(settings.job_schema),
                tbl=sql.Identifier(settings.job_table),
            ),
            (job_id,),
        )
        row = await cur.fetchone()
    if row is None:
        raise HTTPException(404, f"job {job_id} not found")
    return dict(row)


def _serialize_job(row: dict) -> dict:
    out = dict(row)
    if out.get("status") == "interrupted":
        out["verification_hint"] = "调用 GET /maintenance/jobs/{id} 触发核实"
    if out.get("status") == "succeeded":
        out["disk_notice"] = maintenance.DISK_NOTICE
    return out


@app.get(
    "/maintenance/jobs/{job_id}",
    response_model=JobResponse,
    tags=["maintenance"],
    summary="查询作业；interrupted 作业会附带基于统计基线的核实结论",
)
async def get_job(job_id: int) -> JobResponse:
    job = await _load_job(job_id)
    out = _serialize_job(job)
    if job["status"] == "interrupted":
        out["verification_hint"] = await maintenance.verify_interrupted_job(job)
    return JobResponse(**out)


@app.get(
    "/maintenance/jobs",
    tags=["maintenance"],
    summary="列出某表最近的维护作业",
)
async def list_jobs(schema_name: str = "public",
                    table_name: str = "device_events",
                    limit: int = 20) -> dict:
    await _check_relation(schema_name, table_name)
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            sql.SQL(
                "SELECT * FROM {sch}.{tbl} "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY id DESC LIMIT %s"
            ).format(
                sch=sql.Identifier(settings.job_schema),
                tbl=sql.Identifier(settings.job_table),
            ),
            (schema_name, table_name, min(limit, 100)),
        )
        rows = await cur.fetchall()
    return {"jobs": [_serialize_job(dict(r)) for r in rows]}


@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": app.title,
        "docs": "/docs",
        "endpoints": [
            "/health",
            "/diagnostics?schema_name=public&table_name=device_events",
            "/sessions/terminate",
            "/maintenance/vacuum",
            "/maintenance/jobs/{job_id}",
            "/maintenance/jobs",
            "/autovacuum/overrides",
            "/autovacuum/overrides/{id}",
            "/autovacuum/overrides/{id}/restore",
            "/autovacuum/table-setting",
            "/autovacuum/overrides/reconcile",
        ],
    }


# --- 表级 autovacuum 插入阈值临时覆盖 ----------------------------------------

@app.post(
    "/autovacuum/overrides",
    response_model=OverrideActionResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        200: {
            "model": OverrideActionResponse,
            "description": "同一申请号原样重送：返回原记录与原截止时间，"
            "不会重新计时",
        }
    },
    tags=["autovacuum-override"],
    summary="申请临时提高某表按插入量触发 autovacuum 的门槛",
    description=(
        "写入表级 `autovacuum_vacuum_insert_scale_factor` 并登记截止时间。\n\n"
        "- 同一 `request_no` 原样重送 -> 200，返回原记录与原截止时间"
        "（`replayed=true`），不会新开窗口；参数或时长不一致则 409。\n"
        "- 该表已有生效中/待恢复申请 -> 409，旧窗口恢复结束后才受理。\n"
        "- 到期由后台自动恢复申请前的表级设定；原来继承实例默认值的恢复继承。"
    ),
)
async def create_autovacuum_override(
    req: AutovacuumOverrideRequest, response: Response
):
    await _check_relation(req.schema_name, req.table_name)
    try:
        row = await overrides.apply_override(
            req.schema_name,
            req.table_name,
            req.request_no,
            req.autovacuum_vacuum_insert_scale_factor,
            req.duration_seconds,
        )
    except overrides.OverrideError as exc:
        raise _override_http_error(exc) from exc
    if row["replayed"]:
        response.status_code = status.HTTP_200_OK
        row["next_action"] = (
            "同一申请号原样重送，已沿用原记录与原截止时间，未重新计时；"
            + row["next_action"]
        )
        return OverrideActionResponse(
            message="同一申请号已存在且参数一致，沿用原截止时间",
            override=AutovacuumOverrideResponse(**row),
        )
    return OverrideActionResponse(
        message=(
            f"已受理，表级 {overrides.SCALE_FACTOR_OPTION}="
            f"{row['applied_raw_value']}，截止 {row['expires_at']}"
        ),
        override=AutovacuumOverrideResponse(**row),
    )


@app.get(
    "/autovacuum/overrides",
    response_model=OverrideListResponse,
    tags=["autovacuum-override"],
    summary="列出阈值申请记录（可按表/状态过滤）",
)
async def list_autovacuum_overrides(
    schema_name: str | None = Query(None, description="按 schema 过滤"),
    table_name: str | None = Query(None, description="按表名过滤"),
    status_filter: str | None = Query(
        None,
        alias="status",
        pattern="^(active|pending_restore|restored|preserved)$",
        description="按状态过滤",
    ),
    limit: int = Query(50, ge=1, le=200),
) -> OverrideListResponse:
    rows = await overrides.list_overrides(
        schema_name, table_name, status_filter, limit
    )
    return OverrideListResponse(
        overrides=[AutovacuumOverrideResponse(**r) for r in rows]
    )


@app.post(
    "/autovacuum/overrides/reconcile",
    tags=["autovacuum-override"],
    summary="立即处理所有已到期申请（后台轮询之外的手动触发）",
    description=(
        "处理逻辑与后台轮询完全相同；被 VACUUM 等表锁挡住的记录会标记"
        " `pending_restore` 并继续自动重试，不会谎报已恢复。"
    ),
)
async def reconcile_overrides() -> dict:
    handled = await overrides.sweep_due_overrides()
    return {
        "handled": handled,
        "message": "已处理到期申请；pending_restore 表示恢复被阻塞，将自动重试",
    }


@app.get(
    "/autovacuum/table-setting",
    response_model=TableOverrideSettingResponse,
    tags=["autovacuum-override"],
    summary="查询库中该表的实际设定与未结束申请（含截止时间）",
    description=(
        "`insert_scale_factor.source` 直接来自 `pg_class.reloptions`："
        "`table` 表示存在表级设定，`inherited` 表示继承实例默认值。"
        "人工改动会立即反映在此处。"
    ),
)
async def get_table_autovacuum_setting(
    schema_name: str = "public",
    table_name: str = "device_events",
) -> TableOverrideSettingResponse:
    await _check_relation(schema_name, table_name)
    try:
        out = await overrides.describe_table_setting(schema_name, table_name)
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    return TableOverrideSettingResponse(**out)


@app.get(
    "/autovacuum/overrides/{override_id}",
    response_model=AutovacuumOverrideResponse,
    tags=["autovacuum-override"],
    summary="查询单个申请（含状态、截止时间、恢复结果）",
)
async def get_autovacuum_override(override_id: int) -> AutovacuumOverrideResponse:
    try:
        row = await overrides.get_override(override_id)
    except overrides.OverrideError as exc:
        raise _override_http_error(exc) from exc
    return AutovacuumOverrideResponse(**row)


@app.post(
    "/autovacuum/overrides/{override_id}/restore",
    response_model=OverrideActionResponse,
    tags=["autovacuum-override"],
    summary="立即尝试恢复（不等截止时间），供演练/排障",
    description=(
        "仅对 `active`/`pending_restore` 记录有效。会把记录提前置为到期并"
        "走同一恢复流程：参数已被人工改动则 `preserved`；被表锁挡住则"
        " `pending_restore` 并自动重试。"
    ),
)
async def restore_autovacuum_override(override_id: int) -> OverrideActionResponse:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            sql.SQL(
                "UPDATE {sch}.{tbl} SET expires_at = now() "
                "WHERE id = %s AND status IN ('active','pending_restore') "
                "RETURNING id"
            ).format(
                sch=sql.Identifier(settings.job_schema),
                tbl=sql.Identifier(settings.override_table),
            ),
            (override_id,),
        )
        found = await cur.fetchone()
    if found is None:
        raise HTTPException(
            409,
            "记录不存在，或已处于 restored/preserved 终态，无需再次恢复",
        )
    row = await overrides.restore_due_override(override_id)
    if row is None:
        raise HTTPException(409, "记录状态已变化，本次未处理；请重新查询")
    messages = {
        "restored": "已恢复申请前的设定",
        "preserved": "参数已被人工改动，保留现状（见 outcome_detail）",
        "pending_restore": "恢复被表锁阻塞，已标记待恢复，服务会自动重试",
    }
    return OverrideActionResponse(
        message=messages.get(row["status"], row["status"]),
        override=AutovacuumOverrideResponse(**row),
    )


def _override_http_error(exc: overrides.OverrideError) -> HTTPException:
    detail: dict = {"message": exc.detail}
    detail.update(exc.extra)
    return HTTPException(exc.status_code, detail=detail)
