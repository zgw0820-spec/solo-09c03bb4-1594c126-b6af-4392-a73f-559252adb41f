"""全部 SQL 集中在此，动态标识符用 psycopg.sql 安全拼接。

注意: n_dead_tup / n_live_tup 是 *planner 的估算值*，不是精确计数，
也不能被解读为"可释放磁盘字节"。
"""
from __future__ import annotations

from psycopg import sql

from .config import settings


def _q(ident: str) -> sql.Composed:
    return sql.Identifier(ident)


# --- 维护作业表 / 审计表 -----------------------------------------------------

CREATE_SCHEMA = sql.SQL("CREATE SCHEMA IF NOT EXISTS {sch}").format(
    sch=_q(settings.job_schema)
)

CREATE_JOB_TABLE = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {sch}.{tbl} (
        id              bigserial PRIMARY KEY,
        table_schema    text        NOT NULL,
        table_name      text        NOT NULL,
        status          text        NOT NULL CHECK (status IN
                            ('running','succeeded','failed','interrupted')),
        started_at      timestamptz NOT NULL DEFAULT now(),
        finished_at     timestamptz,
        statement_timeout_ms integer NOT NULL,
        -- VACUUM 启动前的统计基线，用于重启后核实是否真的跑过
        before_snapshot jsonb       NOT NULL,
        result          jsonb,
        error_message   text
    )
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

# 同一张表同一时刻只允许一个 running 作业（跨进程/跨重启生效）
CREATE_JOB_UNIQUE = sql.SQL(
    """
    CREATE UNIQUE INDEX IF NOT EXISTS {idx}
        ON {sch}.{tbl} (table_schema, table_name)
        WHERE status = 'running'
    """
).format(
    idx=_q(f"{settings.job_table}_one_running_per_table"),
    sch=_q(settings.job_schema),
    tbl=_q(settings.job_table),
)

CREATE_AUDIT_TABLE = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {sch}.{tbl} (
        id              bigserial PRIMARY KEY,
        occurred_at     timestamptz NOT NULL DEFAULT now(),
        target_pid      integer     NOT NULL,
        claimed_backend_start timestamptz NOT NULL,
        claimed_xact_start    timestamptz NOT NULL,
        decision        text        NOT NULL,
        terminated      boolean     NOT NULL,
        observed_state  jsonb       NOT NULL,
        detail          text
    )
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.audit_table))

# 服务重启/崩溃恢复: 把没有结局的作业标为 interrupted
SWEEP_INTERRUPTED = sql.SQL(
    """
    UPDATE {sch}.{tbl}
       SET status = 'interrupted',
           finished_at = now(),
           error_message = '进程退出或服务重启，VACUUM 结果未经确认；可凭此作业核实或重试'
     WHERE status = 'running'
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

INSERT_JOB = sql.SQL(
    """
    INSERT INTO {sch}.{tbl}
        (table_schema, table_name, status, statement_timeout_ms, before_snapshot)
    VALUES (%s, %s, 'running', %s, %s)
    RETURNING id
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

FINISH_JOB = sql.SQL(
    """
    UPDATE {sch}.{tbl}
       SET status = %s, finished_at = now(), result = %s, error_message = %s
     WHERE id = %s AND status = 'running'
    RETURNING id
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

GET_JOB = sql.SQL(
    "SELECT * FROM {sch}.{tbl} WHERE id = %s"
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

LIST_JOBS = sql.SQL(
    """
    SELECT * FROM {sch}.{tbl}
     WHERE table_schema = %s AND table_name = %s
     ORDER BY id DESC LIMIT %s
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

# --- 业务表诊断 --------------------------------------------------------------

# 表存在性
RELATION_EXISTS = sql.SQL(
    """
    SELECT relpersistence
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = %s AND c.relname = %s
    """
)

# 死元组等统计: 估算值
TABLE_STATS = sql.SQL(
    """
    SELECT current_database() AS db_name,
           n.nspname AS table_schema,
           c.relname AS table_name,
           c.oid::regclass::text AS relation,
           pg_stat_get_dead_tuples(c.oid)  AS n_dead_tup_estimate,
           pg_stat_get_live_tuples(c.oid)  AS n_live_tup_estimate,
           pg_stat_get_vacuum_count(c.oid) AS vacuum_count,
           pg_stat_get_autovacuum_count(c.oid)      AS autovacuum_count,
           stat.last_vacuum,
           stat.last_autovacuum,
           pg_relation_size(c.oid)                 AS relation_size_bytes,
           pg_total_relation_size(c.oid)           AS total_relation_size_bytes,
           GREATEST(pg_stat_get_dead_tuples(c.oid), 0)
               / NULLIF(pg_stat_get_live_tuples(c.oid)
                        + pg_stat_get_dead_tuples(c.oid), 0)
               AS dead_tuple_ratio_estimate,
           stat.last_analyze,
           stat.last_autoanalyze
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN pg_stat_user_tables stat
             ON stat.relid = c.oid
     WHERE n.nspname = %s AND c.relname = %s
    """
)

# 可能阻碍死元组回收的旧快照会话:
# 判据是"事务仍开启"（xact_start 非空且非本维护进程）。
# 注意 READ COMMITTED 下语句结束后快照会注销、backend_xmin 可能为 NULL，
# 但只要事务还开着，下一条语句即可重新持有旧 xmin 视角的快照，
# 因此 idle in transaction 会话仍是最常见的回收阻碍，均列出并标明状态。
BLOCKING_SNAPSHOTS = sql.SQL(
    """
    SELECT pid,
           usename              AS role_name,
           application_name,
           client_addr,
           state,
           backend_start,
           xact_start,
           state_change,
           now() - xact_start   AS xact_age,
           now() - backend_start AS backend_age,
           backend_xid,
           backend_xmin,
           left(query, 200)     AS current_query
      FROM pg_stat_activity
     WHERE datname = current_database()
       AND pid <> pg_backend_pid()
       AND xact_start IS NOT NULL
       AND state <> 'idle'
     ORDER BY xact_start, backend_start
     LIMIT %s
    """
)

# 其它同样可能让 VACUUM 无法回收的因素，一并呈现
PREPARED_XACTS = sql.SQL(
    """
    SELECT transaction AS gid, owner, database,
           prepared, now() - prepared AS age
      FROM pg_prepared_xacts
     WHERE database = current_database()
     ORDER BY prepared
    """
)

REPLICATION_SLOTS = sql.SQL(
    """
    SELECT slot_name, slot_type, active, xmin, catalog_xmin,
           pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes
      FROM pg_replication_slots
     WHERE NOT active
     ORDER BY slot_name
    """
)

# --- 会话终止前的再校验 ------------------------------------------------------

# 只能结束: 同库 + PID + backend_start + xact_start 全部吻合 +
# 仍是 idle in transaction 的原会话，任一变化即拒绝。
RECHECK_SESSION = sql.SQL(
    """
    SELECT pid, datname, state, backend_start, xact_start,
           usename AS role_name, application_name, client_addr,
           left(query, 200) AS current_query
      FROM pg_stat_activity
     WHERE pid = %s
       AND datname = current_database()
       AND backend_start = %s
       AND xact_start = %s
    """
)

AUDIT_TERMINATION = sql.SQL(
    """
    INSERT INTO {sch}.{tbl}
        (target_pid, claimed_backend_start, claimed_xact_start,
         decision, terminated, observed_state, detail)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.audit_table))


# --- 表级 autovacuum 插入阈值临时覆盖 ----------------------------------------
# status:
#   active          已写入并生效，尚未到期
#   pending_restore 已到期，但恢复时等待表锁超时（或其它暂时性错误），自动重试
#   restored        已恢复原表级值 / 已回到继承
#   preserved       到期时发现参数已被人工改走，按规则保留现状并说明
#
# 约束:
#   * (table, request_no) 唯一 —— 同一申请号原样重送返回原记录与原截止时间；
#   * 每张表同时只允许一个 active/pending_restore —— 旧窗口恢复结束前不接受新申请。
OVERRIDE_TABLE = sql.SQL("{sch}.{tbl}").format(
    sch=_q(settings.job_schema), tbl=_q(settings.override_table)
)

CREATE_OVERRIDE_TABLE = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {sch}.{tbl} (
        id              bigserial PRIMARY KEY,
        request_no      text        NOT NULL,
        table_schema    text        NOT NULL,
        table_name      text        NOT NULL,
        requested_scale_factor      double precision NOT NULL,
        requested_duration_seconds  integer      NOT NULL,
        status          text        NOT NULL CHECK (status IN
                            ('active','pending_restore','restored','preserved')),
        applied_at      timestamptz NOT NULL DEFAULT now(),
        expires_at      timestamptz NOT NULL,
        -- 申请生效前该表的原始状态，用于到期恢复
        original_source     text NOT NULL CHECK (original_source IN
                                ('inherited','table')),
        original_raw_value  text,
        applied_raw_value   text NOT NULL,
        restore_attempts    integer NOT NULL DEFAULT 0,
        last_restore_attempt_at timestamptz,
        resolved_at     timestamptz,
        outcome_detail  text,
        last_error      text
    )
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.override_table))

CREATE_OVERRIDE_REQUEST_UNIQUE = sql.SQL(
    """
    CREATE UNIQUE INDEX IF NOT EXISTS {idx}
        ON {sch}.{tbl} (table_schema, table_name, request_no)
    """
).format(
    idx=_q(f"{settings.override_table}_request_no_uq"),
    sch=_q(settings.job_schema),
    tbl=_q(settings.override_table),
)

CREATE_OVERRIDE_ONE_OPEN_PER_TABLE = sql.SQL(
    """
    CREATE UNIQUE INDEX IF NOT EXISTS {idx}
        ON {sch}.{tbl} (table_schema, table_name)
        WHERE status IN ('active','pending_restore')
    """
).format(
    idx=_q(f"{settings.override_table}_one_open_per_table"),
    sch=_q(settings.job_schema),
    tbl=_q(settings.override_table),
)

INSERT_OVERRIDE = sql.SQL(
    """
    INSERT INTO {sch}.{tbl}
        (request_no, table_schema, table_name,
         requested_scale_factor, requested_duration_seconds,
         status, expires_at, original_source, original_raw_value,
         applied_raw_value)
    VALUES (%s, %s, %s, %s, %s, 'active',
            now() + make_interval(secs => %s), %s, %s, %s)
    RETURNING *, now() AS db_now
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.override_table))

GET_OVERRIDE = sql.SQL(
    "SELECT *, now() AS db_now FROM {sch}.{tbl} WHERE id = %s"
).format(sch=_q(settings.job_schema), tbl=_q(settings.override_table))

FIND_OVERRIDE_BY_REQUEST = sql.SQL(
    """
    SELECT *, now() AS db_now FROM {sch}.{tbl}
     WHERE table_schema = %s AND table_name = %s AND request_no = %s
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.override_table))

FIND_OPEN_OVERRIDE_FOR_TABLE = sql.SQL(
    """
    SELECT id, request_no, status, applied_at, expires_at, now() AS db_now
      FROM {sch}.{tbl}
     WHERE table_schema = %s AND table_name = %s
       AND status IN ('active','pending_restore')
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.override_table))

LIST_OVERRIDES = sql.SQL(
    """
    SELECT *, now() AS db_now FROM {sch}.{tbl}
     WHERE (%s::text IS NULL OR table_schema = %s)
       AND (%s::text IS NULL OR table_name = %s)
       AND (%s::text IS NULL OR status = %s)
     ORDER BY id DESC LIMIT %s
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.override_table))

# 到期且尚未进入终态的申请；只取 id，真正处理时再行锁 + 复核
LIST_DUE_OVERRIDE_IDS = sql.SQL(
    """
    SELECT id FROM {sch}.{tbl}
     WHERE status IN ('active','pending_restore')
       AND expires_at <= now()
     ORDER BY expires_at
     LIMIT %s
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.override_table))

# 行锁 + 重新确认仍到期未终态；并发实例/并发轮次只有一个能处理
CLAIM_DUE_OVERRIDE = sql.SQL(
    """
    SELECT *, now() AS db_now FROM {sch}.{tbl}
     WHERE id = %s
       AND status IN ('active','pending_restore')
       AND expires_at <= now()
     FOR UPDATE
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.override_table))

MARK_OVERRIDE = sql.SQL(
    """
    UPDATE {sch}.{tbl}
       SET status = %s,
           restore_attempts = restore_attempts + 1,
           last_restore_attempt_at = now(),
           resolved_at = CASE WHEN %s IN ('restored','preserved') THEN now()
                              ELSE resolved_at END,
           outcome_detail = %s,
           last_error = %s
     WHERE id = %s
    RETURNING *, now() AS db_now
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.override_table))


# --- 读取数据库中的实际表级设定 ----------------------------------------------

# 直接读 pg_class.reloptions，而不是回读本服务记录的值：
# 这样才能看到是否被人工改动、是否已改回继承。
TABLE_INSERT_TRIGGER_SETTING = sql.SQL(
    """
    SELECT n.nspname AS table_schema,
           c.relname AS table_name,
           c.reloptions,
           (SELECT option_value FROM pg_options_to_table(c.reloptions)
             WHERE option_name = 'autovacuum_vacuum_insert_scale_factor')
               AS insert_scale_factor_raw,
           (SELECT option_value FROM pg_options_to_table(c.reloptions)
             WHERE option_name = 'autovacuum_enabled')
               AS autovacuum_enabled_raw,
           current_setting('autovacuum_vacuum_insert_scale_factor')::float8
               AS instance_default_insert_scale_factor,
           current_setting('autovacuum')::boolean AS autovacuum_instance_enabled
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r'
    """
)

