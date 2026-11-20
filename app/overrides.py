"""表级 autovacuum 插入阈值（autovacuum_vacuum_insert_scale_factor）临时覆盖。

业务背景：历史设备事件集中回灌时，``device_events`` 的插入量会持续累积，
值班员需要在一段时间内把该表“按插入量触发 autovacuum”的门槛临时调高，
回灌结束后自动恢复。

状态机（``maintenance.autovacuum_overrides.status``）::

    active ──到期──► restored     （已恢复原始表级值 / 已回到继承）
       │
       └──到期──► pending_restore ──自动重试──► restored / preserved
                        │
                        └────────到期时参数已被人工改走──► preserved

关键事实与取舍：
* 原始状态直接从 ``pg_class.reloptions`` 读取并在申请时冻结：
  ``original_source='inherited'`` 表示原来没有表级设定（继承实例默认值），
  到期恢复即 ``ALTER TABLE ... RESET``；``'table'`` 则恢复为冻结的原始值。
* 恢复前再次读取 ``pg_class.reloptions``：若当前值与本次申请写入的值不一致，
  说明参数已被人工改动（含改回继承），**保留现状**并标记 ``preserved``，
  绝不覆盖人工操作。
* 恢复被表锁挡住（VACUUM、长事务持有 SHARE UPDATE EXCLUSIVE 等）时，
  ``ALTER TABLE`` 会在 ``lock_timeout`` 后失败；此时标记 ``pending_restore``，
  由后台轮询自动重试，绝不谎报已恢复。
* 服务停机跨过期限无需特殊处理：状态与截止时间都在库里，启动后轮询器
  立即发现到期记录并按同一流程恢复。
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from . import db
from . import sql_queries as q
from .config import settings

log = logging.getLogger("uvicorn.error")

SCALE_FACTOR_OPTION = "autovacuum_vacuum_insert_scale_factor"

# 恢复失败时允许自动重试的错误类别
RETRYABLE_SQLSTATES = {
    "55P03",  # lock_not_available（lock_timeout / NOWAIT）
    "40P01",  # deadlock_detected
    "57014",  # query_canceled（statement_timeout）
    "40001",  # serialization_failure
}
RETRYABLE_ERROR_NAMES = {
    "LockNotAvailable",
    "DeadlockDetected",
    "QueryCanceled",
    "SerializationFailure",
}


class OverrideError(Exception):
    """可直接映射为 HTTP 错误的业务异常。"""

    def __init__(self, status_code: int, detail: str, **extra: Any) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.extra = extra


# --- 读取数据库中的实际设定 --------------------------------------------------

async def read_table_setting(
    schema_name: str, table_name: str
) -> dict[str, Any]:
    """返回该表当前的插入阈值来源（表级或继承）与实际值。"""
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            q.TABLE_INSERT_TRIGGER_SETTING, (schema_name, table_name)
        )
        row = await cur.fetchone()
    if row is None:
        raise LookupError(f"relation {schema_name}.{table_name} does not exist")
    row = dict(row)
    raw = row["insert_scale_factor_raw"]
    if raw is None:
        source = "inherited"
        effective = row["instance_default_insert_scale_factor"]
    else:
        source = "table"
        effective = float(raw)
    row["source"] = source
    row["effective_value"] = effective
    return row


def _norm(raw: str | None) -> float | None:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return raw  # type: ignore[return-value]


# --- 申请 --------------------------------------------------------------------

async def apply_override(
    schema_name: str,
    table_name: str,
    request_no: str,
    scale_factor: float,
    duration_seconds: int,
    _attempt: int = 0,
) -> dict[str, Any]:
    """受理申请；同一申请号原样重送幂等返回原记录与原截止时间。

    冲突规则（均带明确 detail，不静默）:
      * 同表同申请号已存在:
          - 参数与时长一致 -> 原样返回该记录（沿用原截止时间）；
          - 参数或时长不一致 -> 409，拒绝悄悄改写；
      * 同表存在 active/pending_restore 的其它申请 -> 409，等旧窗口恢复结束。
    """
    # 存在性/可读性预检；事务内会重新读取以冻结准确原值
    try:
        await read_table_setting(schema_name, table_name)
    except LookupError as exc:
        raise OverrideError(404, "目标表不存在或不是普通表") from exc

    existing = await _find_by_request(schema_name, table_name, request_no)
    if existing is not None:
        same = (
            float(existing["requested_scale_factor"]) == float(scale_factor)
            and int(existing["requested_duration_seconds"]) == int(duration_seconds)
        )
        if same:
            return _row_to_response(existing, replayed=True)
        raise OverrideError(
            409,
            "同一申请号已存在但参数或生效时长不一致；为避免悄悄改写原窗口，拒绝受理",
            existing=_row_to_response(existing, replayed=False),
        )

    open_row = await _find_open_for_table(schema_name, table_name)
    if open_row is not None:
        raise OverrideError(
            409,
            "该表已有生效中或待恢复的阈值申请；旧窗口恢复结束后才受理新申请",
            open_override={
                "id": open_row["id"],
                "request_no": open_row["request_no"],
                "status": open_row["status"],
                "expires_at": open_row["expires_at"].isoformat(),
            },
        )

    applied_raw = _format_factor(scale_factor)

    pool = await db.open_pool()
    try:
        async with pool.connection() as conn:
            # ALTER 与登记在同一事务里：若登记因并发唯一索引冲突失败，
            # ALTER 一并回滚，避免“表被改了但申请没登记”。
            # ALTER TABLE 取 ACCESS EXCLUSIVE 锁，用 lock_timeout 兜底
            #（SET LOCAL 只在事务内生效，因此必须放在 transaction() 内）。
            async with conn.transaction():
                await conn.execute(
                    sql.SQL("SET LOCAL lock_timeout = {}").format(
                        sql.Literal(
                            max(1, settings.override_restore_lock_timeout_seconds)
                            * 1000
                        )
                    )
                )
                # 先拿 ACCESS EXCLUSIVE 锁再读 reloptions：否则可能与另一实例的
                # 恢复交错，把“即将被恢复的原值”误冻结为本申请的 original。
                # 同一事务内持锁完成“读原值 → SET → 登记”，序列化正确。
                await conn.execute(
                    sql.SQL("LOCK TABLE {sch}.{tbl} IN ACCESS EXCLUSIVE MODE").format(
                        sch=sql.Identifier(schema_name),
                        tbl=sql.Identifier(table_name),
                    )
                )
                cur = await conn.execute(
                    q.TABLE_INSERT_TRIGGER_SETTING, (schema_name, table_name)
                )
                current = await cur.fetchone()
                if current is None:
                    raise OverrideError(404, "目标表不存在或不是普通表")
                current = dict(current)
                original_source = (
                    "inherited"
                    if current["insert_scale_factor_raw"] is None
                    else "table"
                )
                original_raw = current["insert_scale_factor_raw"]

                # SAVEPOINT: ALTER 失败只回滚到此处，事务可继续回滚并干净返回
                async with conn.transaction():
                    await _alter_scale_factor(
                        conn, schema_name, table_name, applied_raw
                    )
                cur = await conn.execute(
                    q.INSERT_OVERRIDE,
                    (
                        request_no,
                        schema_name,
                        table_name,
                        float(scale_factor),
                        int(duration_seconds),
                        int(duration_seconds),
                        original_source,
                        original_raw,
                        applied_raw,
                    ),
                )
                row = dict(await cur.fetchone())
    except psycopg.errors.UniqueViolation:
        # 并发重送/并发申请：有限次重判，给出明确结果而非 500/无限递归
        if _attempt >= 2:
            raise OverrideError(
                409, "并发申请冲突，请稍后重新查询该表的申请状态"
            ) from None
        return await apply_override(
            schema_name, table_name, request_no, scale_factor,
            duration_seconds, _attempt + 1,
        )
    except OverrideError:
        raise
    except psycopg.Error as exc:
        raise _translate_alter_error(exc, schema_name, table_name) from exc

    return _row_to_response(row, replayed=False)


def _format_factor(value: float | int) -> str:
    # 0.05 -> "0.05"，1 -> "1"，避免出现 "1.0000000000001" 之类
    return format(float(value), "g")


async def _alter_scale_factor(
    conn: psycopg.AsyncConnection,
    schema_name: str,
    table_name: str,
    value: str,
    reset: bool = False,
) -> None:
    opt = sql.Identifier(SCALE_FACTOR_OPTION)
    if reset:
        # RESET (option) 不接受值
        action = sql.SQL("RESET ({})").format(opt)
    else:
        action = sql.SQL("SET ({} = {})").format(opt, sql.Literal(value))
    await conn.execute(
        sql.SQL("ALTER TABLE {sch}.{tbl} {action}").format(
            sch=sql.Identifier(schema_name),
            tbl=sql.Identifier(table_name),
            action=action,
        )
    )


def _translate_alter_error(
    exc: psycopg.Error, schema_name: str, table_name: str
) -> OverrideError:
    name = type(exc).__name__
    if name == "LockNotAvailable":
        return OverrideError(
            409,
            f"未能取得 {schema_name}.{table_name} 的表锁，参数未改动；"
            "可能有 VACUUM/长事务占用，请稍后重试",
        )
    if name == "UndefinedObject":
        return OverrideError(404, "目标表不存在或不是普通表")
    if name == "InsufficientPrivilege":
        return OverrideError(
            403, "当前数据库用户无权修改该表的存储参数"
        )
    return OverrideError(500, f"修改表存储参数失败: {name}: {exc}")


# --- 查询 --------------------------------------------------------------------

async def _find_by_request(
    schema_name: str, table_name: str, request_no: str
) -> dict[str, Any] | None:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            q.FIND_OVERRIDE_BY_REQUEST, (schema_name, table_name, request_no)
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def _find_open_for_table(
    schema_name: str, table_name: str
) -> dict[str, Any] | None:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            q.FIND_OPEN_OVERRIDE_FOR_TABLE, (schema_name, table_name)
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def get_override(override_id: int) -> dict[str, Any]:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(q.GET_OVERRIDE, (override_id,))
        row = await cur.fetchone()
    if row is None:
        raise OverrideError(404, f"override {override_id} not found")
    return _row_to_response(dict(row), replayed=False)


async def list_overrides(
    schema_name: str | None,
    table_name: str | None,
    status: str | None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            q.LIST_OVERRIDES,
            (
                schema_name, schema_name,
                table_name, table_name,
                status, status,
                max(1, min(limit, 200)),
            ),
        )
        rows = await cur.fetchall()
    return [_serialize_row(dict(r)) for r in rows]


async def describe_table_setting(
    schema_name: str, table_name: str
) -> dict[str, Any]:
    """查询库里实际设定 + 该表未结束的申请及其截止时间。"""
    setting = await read_table_setting(schema_name, table_name)
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            q.FIND_OPEN_OVERRIDE_FOR_TABLE, (schema_name, table_name)
        )
        open_row = await cur.fetchone()
        cur2 = await conn.execute(
            q.LIST_OVERRIDES,
            (schema_name, schema_name, table_name, table_name, None, None, 20),
        )
        history = [dict(r) for r in await cur2.fetchall()]

    now = datetime.now(timezone.utc)
    out: dict[str, Any] = {
        "table_schema": schema_name,
        "table_name": table_name,
        "generated_at": now,
        "insert_scale_factor": {
            "option": SCALE_FACTOR_OPTION,
            "source": setting["source"],
            "effective_value": setting["effective_value"],
            "table_raw_value": setting["insert_scale_factor_raw"],
            "instance_default_value": setting[
                "instance_default_insert_scale_factor"
            ],
            "autovacuum_enabled_table_raw": setting["autovacuum_enabled_raw"],
            "autovacuum_instance_enabled": setting["autovacuum_instance_enabled"],
            "source_explanation": (
                "inherited: 该表没有表级设定，当前继承实例默认值；"
                "table: 该表存在表级设定（reloptions）"
            ),
        },
        "open_override": None,
        "history": [_serialize_row(r) for r in history],
    }
    if open_row is not None:
        exp = open_row["expires_at"]
        out["open_override"] = {
            "id": open_row["id"],
            "request_no": open_row["request_no"],
            "status": open_row["status"],
            "applied_at": open_row["applied_at"].isoformat(),
            "expires_at": exp.isoformat(),
            "seconds_until_expiry": max(
                0.0, (exp - now).total_seconds()
            ),
            "note": (
                "status=active 表示生效中；pending_restore 表示已到期但恢复"
                "被阻塞，服务会自动重试"
            ),
        }
    return out


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    now = out.get("db_now")
    for k, v in list(out.items()):
        if isinstance(v, datetime):
            out[k] = v.isoformat()
    if now is not None and out.get("expires_at"):
        try:
            exp = datetime.fromisoformat(out["expires_at"])
            out["seconds_until_expiry"] = max(0.0, (exp - now).total_seconds())
            out["is_due"] = exp <= now
        except ValueError:
            pass
    if "requested_duration_seconds" in out:
        out["duration_seconds"] = int(out["requested_duration_seconds"])
    if "requested_scale_factor" in out:
        out["requested_scale_factor"] = float(out["requested_scale_factor"])
    if "status" in out:
        out["next_action"] = _next_action_hint(out)
    return out


def _row_to_response(row: dict[str, Any], replayed: bool) -> dict[str, Any]:
    out = _serialize_row(row)
    out["replayed"] = replayed
    return out


def _next_action_hint(row: dict[str, Any]) -> str:
    status = row["status"]
    if status == "active":
        return "生效中；到期后服务会自动恢复原设定（或恢复继承）"
    if status == "pending_restore":
        return "已到期，恢复暂时被阻塞（通常为表锁），服务会按间隔自动重试"
    if status == "restored":
        return "已恢复申请前的设定"
    if status == "preserved":
        return "到期时参数已被人工改动，按规则保留现状，未覆盖人工操作"
    return ""


# --- 到期恢复 ----------------------------------------------------------------

def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, psycopg.errors.LockNotAvailable):
        return True
    if isinstance(exc, psycopg.errors.DeadlockDetected):
        return True
    if isinstance(exc, psycopg.errors.QueryCanceled):
        return True
    if isinstance(exc, psycopg.errors.SerializationFailure):
        return True
    name = type(exc).__name__
    if name in RETRYABLE_ERROR_NAMES:
        return True
    sqlstate = getattr(exc, "sqlstate", None)
    return bool(sqlstate and sqlstate in RETRYABLE_SQLSTATES)


async def restore_due_override(override_id: int) -> dict[str, Any] | None:
    """尝试恢复一条已到期的申请。

    返回处理后的记录；若记录不再到期/已被他处处理则返回 None。
    使用独立连接 + 行锁，确保多实例/多轮询并发时同一记录只处理一次。
    """
    deadline_ms = max(1, settings.override_restore_lock_timeout_seconds) * 1000
    async with await psycopg.AsyncConnection.connect(
        settings.database_url, autocommit=True, row_factory=dict_row
    ) as conn:
        # 只让“取表锁”这一步受限；后面的记录更新不受影响
        await conn.execute(
            sql.SQL("SET lock_timeout = {}").format(sql.Literal(deadline_ms))
        )
        async with conn.transaction():
            cur = await conn.execute(q.CLAIM_DUE_OVERRIDE, (override_id,))
            row = await cur.fetchone()
            if row is None:
                return None
            row = dict(row)
            outcome = await _restore_one(conn, row)
            cur2 = await conn.execute(
                q.MARK_OVERRIDE,
                (
                    outcome["status"],
                    outcome["status"],
                    outcome["detail"],
                    outcome.get("error"),
                    override_id,
                ),
            )
            return _row_to_response(
                dict(await cur2.fetchone()), replayed=False
            )


async def _restore_one(
    conn: psycopg.AsyncConnection, row: dict[str, Any]
) -> dict[str, Any]:
    schema_name = row["table_schema"]
    table_name = row["table_name"]
    applied_raw = row["applied_raw_value"]
    attempts = int(row["restore_attempts"]) + 1

    # 0) 先在 lock_timeout 内取得 ACCESS EXCLUSIVE 锁，再读现状：
    #    否则“读到旧值 -> 人工改成别的值 -> 我们写回”会把人工改动覆盖掉。
    #    取锁失败视为暂时性阻塞，交由调用方标记 pending_restore 自动重试。
    try:
        async with conn.transaction():
            await conn.execute(
                sql.SQL(
                    "LOCK TABLE {sch}.{tbl} IN ACCESS EXCLUSIVE MODE"
                ).format(
                    sch=sql.Identifier(schema_name),
                    tbl=sql.Identifier(table_name),
                )
            )
    except psycopg.Error as exc:
        if not _is_retryable(exc):
            log.warning("恢复 overrides id=%s 取表锁失败: %s", row["id"], exc)
        return {
            "status": "pending_restore",
            "detail": (
                f"已到期但未能取得表锁（第 {attempts} 次尝试）: "
                f"{type(exc).__name__}，服务将自动重试"
            ),
            "error": f"{type(exc).__name__}: {exc}",
        }

    # 1) 现状确认: 参数是否仍是我们写入的值？（已持锁，读到的值不会被并发改走）
    cur = await conn.execute(
        q.TABLE_INSERT_TRIGGER_SETTING, (schema_name, table_name)
    )
    current = await cur.fetchone()
    if current is None:
        return {
            "status": "preserved",
            "detail": "到期恢复时目标表已不存在，无法恢复，保留记录并说明",
            "error": None,
        }
    current = dict(current)
    current_raw = current["insert_scale_factor_raw"]

    if current_raw is None:
        return {
            "status": "preserved",
            "detail": (
                "到期时该表已没有表级 autovacuum_vacuum_insert_scale_factor "
                "设定（已被人工改回继承），保留现状，不覆盖人工操作"
            ),
            "error": None,
        }
    if _norm(current_raw) != _norm(applied_raw):
        return {
            "status": "preserved",
            "detail": (
                f"到期时参数已被人工改为 {current_raw}（本申请写入的是 "
                f"{applied_raw}），保留现状，不覆盖人工操作"
            ),
            "error": None,
        }

    # 2) 执行恢复（表锁在 lock_timeout 内取不到会抛 LockNotAvailable）
    #    用 SAVEPOINT 包住: ALTER 失败会让当前事务进入 aborted 状态，
    #    若不回滚到保存点，连“标记 pending_restore”的 UPDATE 也无法提交。
    outcome: dict[str, Any]
    try:
        async with conn.transaction():
            if row["original_source"] == "inherited":
                await _alter_scale_factor(
                    conn, schema_name, table_name, "", reset=True
                )
                detail = (
                    "已到期并恢复: 申请前该表继承实例默认值，已 RESET 表级设定，"
                    "重新继承当前实例默认值 "
                    f"{current['instance_default_insert_scale_factor']}"
                )
            else:
                original = row["original_raw_value"]
                await _alter_scale_factor(conn, schema_name, table_name, original)
                detail = (
                    f"已到期并恢复为申请前的表级设定 "
                    f"{SCALE_FACTOR_OPTION}={original}"
                )
        outcome = {"status": "restored", "detail": detail, "error": None}
    except psycopg.Error as exc:
        if not _is_retryable(exc):
            # 非锁类错误也不假装成功: 标记待恢复，保留错误并继续自动重试
            log.warning("恢复 overrides id=%s 失败: %s", row["id"], exc)
        outcome = {
            "status": "pending_restore",
            "detail": (
                f"已到期但恢复未完成（第 {attempts} 次尝试）: "
                f"{type(exc).__name__}，服务将自动重试"
            ),
            "error": f"{type(exc).__name__}: {exc}",
        }
    return outcome


async def sweep_due_overrides(limit: int = 50) -> int:
    """处理所有已到期的申请，返回本轮处理条数。"""
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(q.LIST_DUE_OVERRIDE_IDS, (limit,))
        ids = [r["id"] for r in await cur.fetchall()]
    handled = 0
    for oid in ids:
        try:
            if await restore_due_override(oid) is not None:
                handled += 1
        except Exception:  # 单个失败不影响其它记录；下一轮继续
            log.exception("处理 overrides id=%s 时出错", oid)
    return handled
