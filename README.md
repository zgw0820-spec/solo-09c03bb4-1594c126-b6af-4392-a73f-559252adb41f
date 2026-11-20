# PostgreSQL 16 设备事件表 VACUUM 维护 API

值班员在 `device_events` 等表删除旧数据后，用于：查看死元组**估算**与可能阻碍
回收的旧快照会话 → 受控地终止原会话 → 执行 VACUUM 并回读统计结果。

另外提供**表级 autovacuum 插入阈值临时覆盖**：历史设备事件集中回灌时，
临时提高 `autovacuum_vacuum_insert_scale_factor`，到期自动恢复原表级设定
（原来继承实例默认值的恢复继承）。

技术栈：FastAPI + psycopg 3 + uvicorn，PostgreSQL 16，Docker Compose 一键启动。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:      http://localhost:8000
# Swagger:  http://localhost:8000/docs
# Postgres: localhost:55432（库 devicedb，用户 appuser/apppass）
```

初始化脚本 `db/init/01_device_events.sql` 会建 `public.device_events`，
插入 10 万行后删除一批行制造死元组。该演示表的 **autovacuum 保持开启**，
且**不设置**表级 `autovacuum_vacuum_insert_scale_factor`（初始为“继承实例
默认值”）。为避免 autovacuum 在演示前就把死元组清掉，只把 `autovacuum_vacuum_*`
触发阈值调高（与插入阈值无关）。

## 接口

| 方法/路径 | 说明 |
| --- | --- |
| `GET /health` | 数据库连通性 |
| `GET /diagnostics?schema_name=public&table_name=device_events` | 死元组/活元组**估算**、最后 VACUUM 时间、表当前占用字节、可能阻碍回收的旧快照会话（含 `pid`/`backend_start`/`xact_start`）、预备事务与非活跃复制槽 |
| `POST /sessions/terminate` | 凭诊断返回的 `pid`+`backend_start`+`xact_start` 终止会话；再校验失败即拒绝 |
| `POST /maintenance/vacuum` | 对表执行 `VACUUM (VERBOSE, ANALYZE)`，同表单飞，返回作业 id |
| `GET /maintenance/jobs/{id}` | 查询作业；`interrupted` 作业附带基于统计基线的核实结论 |
| `GET /maintenance/jobs` | 某表最近的作业 |
| `POST /autovacuum/overrides` | 申请表级插入阈值临时覆盖（申请号 + 系数 + 生效时长）|
| `GET /autovacuum/overrides` | 列出申请记录，可按表/状态过滤 |
| `GET /autovacuum/overrides/{id}` | 单条申请状态、截止时间、恢复结果 |
| `POST /autovacuum/overrides/{id}/restore` | 立即尝试恢复（不等截止时间），供演练/排障 |
| `POST /autovacuum/overrides/reconcile` | 立即处理全部到期申请（后台轮询之外的手动触发）|
| `GET /autovacuum/table-setting` | **库中实际设定**（`pg_class.reloptions`）+ 未结束申请与截止时间 |

### 典型流程

```bash
# 1) 诊断：拿到死元组估算与阻碍会话
curl -s http://localhost:8000/diagnostics | jq

# 2) 仅当确认某个 idle in transaction 会话应终止时，原样回传三个字段
curl -s -X POST http://localhost:8000/sessions/terminate \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"device_events","pid":123,
       "backend_start":"2026-11-19T01:00:00+00:00",
       "xact_start":"2026-11-19T01:01:00+00:00"}'

# 3) 执行 VACUUM（异步受理，凭 job_id 轮询；也可 wait=true 同步等）
curl -s -X POST http://localhost:8000/maintenance/vacuum \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"device_events"}'

curl -s http://localhost:8000/maintenance/jobs/1 | jq
```

### 阈值临时覆盖示例

```bash
# 申请：回灌期间把 device_events 的插入触发门槛调高，生效 2 小时
curl -s -X POST http://localhost:8000/autovacuum/overrides \
  -H 'Content-Type: application/json' \
  -d '{"request_no":"CHG-20261120-001",
       "table_name":"device_events",
       "autovacuum_vacuum_insert_scale_factor":0.05,
       "duration_seconds":7200}'

# 查库中实际设定与未结束申请的截止时间
curl -s 'http://localhost:8000/autovacuum/table-setting?table_name=device_events' | jq

# 同一申请号原样重送：200 + replayed=true，沿用原截止时间，不重新计时
curl -s -X POST http://localhost:8000/autovacuum/overrides \
  -H 'Content-Type: application/json' \
  -d '{"request_no":"CHG-20261120-001",
       "table_name":"device_events",
       "autovacuum_vacuum_insert_scale_factor":0.05,
       "duration_seconds":7200}'
```

## 关键设计与事实边界

1. **终止会话的再校验（默认拒绝）**：执行 `pg_terminate_backend` 前重新查询
   `pg_stat_activity`，必须同时满足：同库、PID 存在、`backend_start` 一致、
   `xact_start` 一致、`state = 'idle in transaction'`。会话已退出、被替换
   （PID 复用但 backend_start 已变）、事务重开（xact_start 已变）或状态改变，
   一律拒绝。所有尝试（含拒绝）写入 `maintenance.termination_audit`。
2. **同表并发维护不重复执行**：
   - 持久化第一道保险：`maintenance.vacuum_jobs` 上的部分唯一索引
     `WHERE status='running'`，同表第二个请求直接收到 `409`；
   - 进程内/跨进程第二道保险：按表派生 key 的 session-level advisory lock。
3. **超时/重启绝不虚报完成**：
   - 作业状态只有 `running/succeeded/failed/interrupted`，结果只由后台执行器
     在 VACUUM 真正返回后落库；HTTP 请求超时只影响调用方等待，不改变作业状态；
   - 服务启动时把遗留的 `running` 全部标记为 `interrupted`，并在查询时对比
     作业登记的统计基线（`vacuum_count`、死元组估算）给出核实结论，支持重试；
   - 重试会新建作业，不覆盖历史记录。
4. **估值不是磁盘释放量**：`n_dead_tup_estimate`/`n_live_tup_estimate` 是
   规划器统计估算（PG16 中由统计子系统异步投递，存在可见延迟）；
   `relation_size_bytes` 是当前占用。普通 `VACUUM` 把空间留在库内复用，
   通常**不向操作系统归还磁盘**，因此 API 不提供也不使用“已释放磁盘字节”
   之类的字段或表述。
5. **VACUUM 不能在事务块中运行**：执行器使用独立的 autocommit 连接。
6. **表级插入阈值覆盖的状态机与边界**（`maintenance.autovacuum_overrides`）：
   - `active → restored`：到期把表级设定恢复为申请前冻结的值；申请前是
     “继承实例默认值”的，恢复即 `RESET` 表级设定，重新继承**当前**实例默认值
     （不冻结实例默认值本身）。原始来源与值在申请时从 `pg_class.reloptions`
     读取并冻结，恢复不依赖服务内存状态，因此**停机跨过期限后启动即恢复**。
   - 幂等：`(表, 申请号)` 唯一。同一申请号、参数与时长一致 -> 返回原记录与
     原截止时间（HTTP 200，`replayed=true`）；参数/时长不一致 -> 409，绝不
     悄悄改写原窗口。每张表同时只允许一条 `active/pending_restore`，旧窗口
     恢复结束（`restored/preserved`）后才受理新申请。
   - 恢复被表锁挡住（如正在 `VACUUM` 持有 `SHARE UPDATE EXCLUSIVE`）时，
     `ALTER TABLE` 在 `lock_timeout` 后失败，记录标记 `pending_restore` 并由
     后台轮询自动重试，**不谎报已恢复**；期间仍占用该表的申请名额。
   - 到期时若参数已被人工改成别的值、或已改回继承，则保留现状并标记
     `preserved`，在 `outcome_detail` 中说明检测到的值，**不覆盖人工操作**。
   - 申请时同样在 `ALTER` 前复核现状，`ALTER` 与登记同事务，失败整体回滚；
     取不到表锁直接 409，不会出现“表改了但没登记”。
   - 说明：表级覆盖与实例级 `autovacuum` 总开关相互独立；若实例 `autovacuum=off`，
     设置了表级插入阈值也不会触发 autovacuum。`GET /autovacuum/table-setting`
     会同时给出 `autovacuum_instance_enabled` 与表级 `autovacuum_enabled` 原值。

## 本地自测（无需 Docker）

`tests/selftest.py` 对真实 PostgreSQL 16 实例做端到端验证，覆盖：
诊断字段、伪造/过期凭证拒绝、会话状态改变拒绝、会话替换拒绝、正常终止与审计、
VACUUM 成功路径与统计回读、同表 409 单飞、重启后 interrupted 核实与重试、
statement_timeout 失败落库后可重新发起。

`tests/override_selftest.py` 覆盖阈值覆盖的全部要求（42 项断言）：
申请生效与库中实际值回读、幂等重送沿用截止时间、参数冲突 409、旧窗口未结束
拒收新申请、到期恢复继承/恢复原表级值、应用重启跨期、人工改值/改回继承时
保留现状、被表锁挡住 `pending_restore` 并自动重试、待恢复期间拒收新申请、
并发申请只成功一个。

`tests/restart_demo.sh` 用真实 uvicorn 进程演练“停机跨过期限”：启动服务 →
申请 3 秒窗口 → 杀进程等 5 秒 → 重启 → 验证启动即恢复。

```bash
# 需要一个可达的 PG16 实例（已执行 db/init/01_device_events.sql）
export DATABASE_URL='postgresql://user:pass@localhost:5432/devicedb'
python3 tests/selftest.py
python3 tests/override_selftest.py
bash tests/restart_demo.sh        # 可用 API_PORT 指定端口
```

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://postgres:postgres@db:5432/devicedb` | 维护连接串 |
| `MAINT_SCHEMA` | `maintenance` | 作业/审计表 schema（API 自建） |
| `VACUUM_STATEMENT_TIMEOUT_MS` | `0`（不限） | VACUUM 语句超时 |
| `SYNC_WAIT_SECONDS` | `30` | `wait=true` 最长等待秒数 |
| `POOL_MIN_SIZE` / `POOL_MAX_SIZE` | `1` / `10` | 连接池容量 |
| `MAINT_OVERRIDE_TABLE` | `autovacuum_overrides` | 阈值申请记录表名 |
| `OVERRIDE_RECONCILE_INTERVAL_SECONDS` | `1` | 到期恢复轮询间隔 |
| `OVERRIDE_RESTORE_LOCK_TIMEOUT_SECONDS` | `3` | 单次恢复/申请取表锁上限 |
| `OVERRIDE_SCALE_FACTOR_MIN` / `MAX` | `0` / `100` | 申请值允许区间（与 PG16 一致）|
| `OVERRIDE_MAX_DURATION_HOURS` | `720` | 单次生效时长上限 |
