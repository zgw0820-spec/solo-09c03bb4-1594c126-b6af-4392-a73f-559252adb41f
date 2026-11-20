"""表级 autovacuum 插入阈值临时覆盖 —— 端到端自测。

对真实 PostgreSQL 16 运行，覆盖:
  1. 申请后库中实际设定生效；查询接口回读来源与截止时间
  2. 同一申请号原样重送沿用原截止时间；参数不一致拒绝
  3. 旧窗口未结束时新申请被拒；恢复结束后可受理
  4. 到期自动恢复为原表级值
  5. 原为继承 -> 到期恢复继承
  6. 服务停机跨过期限 -> 启动后恢复
  7. 到期时参数被人工改走 / 改回继承 -> 保留现状并说明
  8. 恢复被 VACUUM 表锁挡住 -> pending_restore，解锁后自动重试成功

运行:
    DATABASE_URL='postgresql://...' python3 -I tests/override_selftest.py
"""
from __future__ import annotations

import os
import sys
import threading
import time

import psycopg
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
results: list[tuple[str, bool, str]] = []

TABLE = "device_events"
OPT = "autovacuum_vacuum_insert_scale_factor"


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((name, bool(cond), detail))
    print(f"  [{PASS if cond else FAIL}] {name}" + (f" — {detail}" if detail else ""))


def admin() -> psycopg.Connection:
    return psycopg.connect(settings.database_url, autocommit=True)


def raw_factor(conn: psycopg.Connection, table: str = TABLE) -> str | None:
    row = conn.execute(
        "SELECT (SELECT option_value FROM pg_options_to_table(c.reloptions) "
        "WHERE option_name=%s) FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='public' AND c.relname=%s",
        (OPT, table),
    ).fetchone()
    return row[0] if row else None


def reset_table(conn: psycopg.Connection) -> None:
    conn.execute(f"ALTER TABLE public.{TABLE} RESET ({OPT})")


def expire(conn: psycopg.Connection, override_id: int) -> None:
    conn.execute(
        "UPDATE maintenance.autovacuum_overrides SET expires_at=now() "
        "WHERE id=%s",
        (override_id,),
    )


def wait_status(client: TestClient, oid: int, statuses: set[str],
                timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    body: dict = {}
    while time.time() < deadline:
        body = client.get(f"/autovacuum/overrides/{oid}").json()
        if body["status"] in statuses:
            return body
        time.sleep(0.15)
    return body


def create(client: TestClient, request_no: str, factor: float, dur: int,
           table: str = TABLE):
    return client.post("/autovacuum/overrides", json={
        "request_no": request_no,
        "table_name": table,
        "autovacuum_vacuum_insert_scale_factor": factor,
        "duration_seconds": dur,
    })


def main() -> int:
    # 快速轮询，便于观察自动重试
    settings.override_reconcile_interval_seconds = 0.3
    settings.override_restore_lock_timeout_seconds = 2

    with admin() as adm:
        adm.execute("DELETE FROM maintenance.autovacuum_overrides")
        reset_table(adm)
        # 记录实例默认值供继承断言
    with admin() as adm:
        inst_default = adm.execute(
            f"SHOW {OPT}"
        ).fetchone()[0]

    with TestClient(app) as client:
        print("\n== 1. 申请生效 & 查询实际设定 ==")
        r = create(client, "REQ-A", 0.02, 3600)
        check("申请受理 201", r.status_code == 201, r.text)
        body = r.json()["override"]
        oid_a = body["id"]
        check("响应 original_source=inherited（原为继承）",
              body["original_source"] == "inherited")
        check("响应带截止时间", bool(body["expires_at"]))
        with admin() as adm:
            check("库中 reloptions 已写入表级值",
                  raw_factor(adm) == "0.02", str(raw_factor(adm)))
        s = client.get("/autovacuum/table-setting",
                       params={"table_name": TABLE}).json()
        check("查询接口反映 source=table 且 effective=0.02",
              s["insert_scale_factor"]["source"] == "table"
              and s["insert_scale_factor"]["effective_value"] == 0.02,
              str(s["insert_scale_factor"]))
        check("查询接口给出未结束申请与截止时间",
              s["open_override"]["id"] == oid_a
              and s["open_override"]["status"] == "active"
              and s["open_override"]["seconds_until_expiry"] > 0)

        print("\n== 2. 幂等重送 & 参数冲突 ==")
        expires_a = body["expires_at"]
        time.sleep(1.0)
        r2 = create(client, "REQ-A", 0.02, 3600)
        check("原样重送返回 201/200 且 replayed=true",
              r2.status_code in (200, 201) and r2.json()["override"]["replayed"],
              r2.text)
        check("原样重送沿用原截止时间",
              r2.json()["override"]["expires_at"] == expires_a,
              f"{r2.json()['override']['expires_at']} vs {expires_a}")
        check("原样重送不新建记录", r2.json()["override"]["id"] == oid_a)
        r3 = create(client, "REQ-A", 0.5, 3600)
        check("同申请号改参数 -> 409", r3.status_code == 409, r3.text)

        print("\n== 3. 旧窗口未结束不收新申请 ==")
        r4 = create(client, "REQ-B", 0.05, 3600)
        check("该表已有生效申请 -> 新申请 409", r4.status_code == 409, r4.text)
        check("409 附带旧申请状态",
              r4.json()["detail"]["open_override"]["id"] == oid_a)

        print("\n== 4. 到期自动恢复原表级值 ==")
        with admin() as adm:
            expire(adm, oid_a)
        got = wait_status(client, oid_a, {"restored"})
        check("到期后状态 restored", got["status"] == "restored",
              got.get("last_error") or "")
        with admin() as adm:
            check("恢复后回到继承（reloptions 无该键）",
                  raw_factor(adm) is None, str(raw_factor(adm)))
        check("恢复说明写明恢复继承",
              "继承" in (got.get("outcome_detail") or ""),
              got.get("outcome_detail") or "")
        s = client.get("/autovacuum/table-setting",
                       params={"table_name": TABLE}).json()
        check("查询接口恢复 source=inherited 且 effective=实例默认",
              s["insert_scale_factor"]["source"] == "inherited"
              and s["insert_scale_factor"]["effective_value"] == float(inst_default),
              str(s["insert_scale_factor"]))
        check("旧窗口结束后 open_override 为 null", s["open_override"] is None)
        r5 = create(client, "REQ-C", 0.07, 3600)
        check("旧窗口恢复结束后可受理新申请", r5.status_code == 201, r5.text)
        oid_c = r5.json()["override"]["id"]

        print("\n== 5. 原为表级值 -> 到期恢复表级值 ==")
        # 先把该窗口提前结束，再制造一个“申请前已有的表级值” .02
        with admin() as adm:
            expire(adm, oid_c)
        wait_status(client, oid_c, {"restored"})
        with admin() as adm:
            adm.execute(f"ALTER TABLE public.{TABLE} SET ({OPT}=0.02)")
        r5 = create(client, "REQ-C2", 0.13, 3600)
        oid_c = r5.json()["override"]["id"]
        check("申请记录 original_source=table 且冻结原值",
              r5.json()["override"]["original_source"] == "table"
              and r5.json()["override"]["original_raw_value"] == "0.02",
              str(r5.json()["override"]["original_source"]))
        with admin() as adm:
            expire(adm, oid_c)
        got = wait_status(client, oid_c, {"restored"})
        with admin() as adm:
            check("恢复为申请前表级值 0.02",
                  raw_factor(adm) == "0.02", str(raw_factor(adm)))
        # 清场
        with admin() as adm:
            reset_table(adm)

    # 独立的应用实例 == 服务停机后重启；全局连接池不能跨事件循环复用，
    # 因此必须在第一个 TestClient 退出之后再进入第二个。
    with admin() as adm:
        adm.execute(
            "UPDATE maintenance.autovacuum_overrides SET expires_at=now() "
            "WHERE status IN ('active','pending_restore')"
        )
    with TestClient(app) as client:
        print("\n== 6. 停机跨过期限 ==")
        r = create(client, "REQ-D", 0.11, 1)
        oid_d = r.json()["override"]["id"]
        with admin() as adm:
            # 模拟停机期间时间流逝：直接推到过去
            expire(adm, oid_d)

    with TestClient(app) as client2:
        got = wait_status(client2, oid_d, {"restored"}, timeout=10)
        check("重启后立即恢复过期申请", got["status"] == "restored",
              got.get("last_error") or "")
        with admin() as adm2:
            check("重启恢复后 reloptions 无该键",
                  raw_factor(adm2) is None, str(raw_factor(adm2)))

        print("\n== 7. 到期时参数被人工改走 ==")
        r = create(client2, "REQ-E", 0.03, 3600)
        oid_e = r.json()["override"]["id"]
        with admin() as adm:
            adm.execute(f"ALTER TABLE public.{TABLE} SET ({OPT}=0.9)")
            expire(adm, oid_e)
        got = wait_status(client2, oid_e, {"preserved"})
        check("人工改值 -> preserved", got["status"] == "preserved",
              got.get("last_error") or "")
        check("说明含人工改动后的值",
              "0.9" in (got.get("outcome_detail") or ""),
              got.get("outcome_detail") or "")
        with admin() as adm:
            check("人工值被保留，未被覆盖",
                  raw_factor(adm) == "0.9", str(raw_factor(adm)))
            reset_table(adm)

        print("\n== 8. 到期时已改回继承 ==")
        r = create(client2, "REQ-F", 0.04, 3600)
        oid_f = r.json()["override"]["id"]
        with admin() as adm:
            reset_table(adm)
            expire(adm, oid_f)
        got = wait_status(client2, oid_f, {"preserved"})
        check("改回继承 -> preserved", got["status"] == "preserved")
        check("说明提到已无表级设定/改回继承",
              "继承" in (got.get("outcome_detail") or ""),
              got.get("outcome_detail") or "")
        with admin() as adm:
            check("保持继承，不重新写表级值", raw_factor(adm) is None)

        print("\n== 9. 恢复被 VACUUM 表锁挡住 -> pending_restore -> 自动重试 ==")
        with admin() as adm:
            adm.execute("DROP TABLE IF EXISTS public.av_lock_t")
            adm.execute(
                "CREATE TABLE public.av_lock_t(id int) "
                "WITH (autovacuum_enabled=true)"
            )
        r = create(client2, "REQ-G2", 0.06, 3600, table="av_lock_t")
        oid_g2 = r.json()["override"]["id"]

        # 另一会话持有 SHARE UPDATE EXCLUSIVE（与 VACUUM 相同的锁模式），
        # ALTER TABLE 需要的 ACCESS EXCLUSIVE 会被挡住。
        holder = psycopg.connect(settings.database_url, autocommit=True)
        vac_started = threading.Event()
        vac_release = threading.Event()

        def hold_vacuum(conn: psycopg.Connection) -> None:
            conn.execute("BEGIN")
            conn.execute(
                "LOCK TABLE public.av_lock_t IN SHARE UPDATE EXCLUSIVE MODE"
            )
            vac_started.set()
            vac_release.wait(30)
            conn.execute("ROLLBACK")

        t = threading.Thread(target=hold_vacuum, args=(holder,))
        t.start()
        vac_started.wait(5)
        with admin() as adm:
            expire(adm, oid_g2)
        got = wait_status(client2, oid_g2, {"pending_restore", "restored"}, timeout=10)
        check("被表锁挡住 -> pending_restore",
              got["status"] == "pending_restore", got.get("last_error") or "")
        check("pending_restore 保留错误并标注重试",
              "自动重试" in (got.get("outcome_detail") or ""),
              got.get("outcome_detail") or "")
        check("待恢复期间参数仍是申请值（未谎报恢复）",
              got["status"] != "restored")
        attempts_before = got["restore_attempts"]
        # 单次恢复尝试最长会等 lock_timeout(2s)，因此多给一些时间观察重试
        deadline = time.time() + 8
        retried = attempts_before
        while time.time() < deadline:
            retried = client2.get(
                f"/autovacuum/overrides/{oid_g2}"
            ).json()["restore_attempts"]
            if retried > attempts_before:
                break
            time.sleep(0.2)
        check("待恢复期间持续自动重试",
              retried > attempts_before,
              f"{attempts_before} -> {retried}")
        new_req = create(client2, "REQ-G3", 0.09, 3600, table="av_lock_t")
        check("待恢复期间不受理该表新申请 -> 409",
              new_req.status_code == 409, new_req.text[:200])
        vac_release.set()
        t.join(10)
        holder.close()
        got3 = wait_status(client2, oid_g2, {"restored"}, timeout=15)
        check("解除锁后自动恢复成功", got3["status"] == "restored",
              got3.get("last_error") or "")
        with admin() as adm:
            check("恢复后 av_lock_t 回到继承", raw_factor(adm, "av_lock_t") is None)

        print("\n== 10. 参数校验 ==")
        bad = client2.post("/autovacuum/overrides", json={
            "request_no": "REQ-X", "table_name": TABLE,
            "autovacuum_vacuum_insert_scale_factor": 1000,
            "duration_seconds": 60,
        })
        check("超出 PG 取值范围 -> 422", bad.status_code == 422, bad.text[:200])
        bad2 = client2.post("/autovacuum/overrides", json={
            "request_no": "REQ-Y", "table_name": TABLE,
            "autovacuum_vacuum_insert_scale_factor": 0.1,
            "duration_seconds": 0,
        })
        check("时长为 0 -> 422", bad2.status_code == 422, bad2.text[:200])
        bad3 = client2.post("/autovacuum/overrides", json={
            "request_no": "REQ-Z", "table_name": "no_such_table",
            "autovacuum_vacuum_insert_scale_factor": 0.1,
            "duration_seconds": 60,
        })
        check("表不存在 -> 404", bad3.status_code == 404, bad3.text[:200])

        print("\n== 11. 列表与手动恢复 ==")
        r = create(client2, "REQ-H", 0.08, 3600)
        oid_h = r.json()["override"]["id"]
        lst = client2.get("/autovacuum/overrides",
                          params={"table_name": TABLE, "status": "active"}).json()
        check("列表按状态过滤且包含新申请",
              any(o["id"] == oid_h for o in lst["overrides"]))
        rr = client2.post(f"/autovacuum/overrides/{oid_h}/restore")
        check("手动恢复接口返回 restored",
              rr.status_code == 200 and rr.json()["override"]["status"] == "restored",
              rr.text[:200])
        rr2 = client2.post(f"/autovacuum/overrides/{oid_h}/restore")
        check("终态记录再次恢复 -> 409", rr2.status_code == 409, rr2.text[:200])

        print("\n== 12. 并发申请同表只成功一个 ==")
        import concurrent.futures

        def go(i):
            return create(client2, f"RACE-{i}", 0.1, 3600).status_code

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            codes = sorted(ex.map(go, range(4)))
        check("并发 4 个申请仅 1 个受理、其余 409",
              codes.count(201) == 1 and codes.count(409) == 3, str(codes))

    with admin() as adm:
        adm.execute(
            "UPDATE maintenance.autovacuum_overrides SET expires_at=now() "
            "WHERE status IN ('active','pending_restore')"
        )
        reset_table(adm)
        adm.execute("DROP TABLE IF EXISTS public.av_lock_t")

    # 让最后一轮 pending/active 记录有机会收尾
    with TestClient(app):
        time.sleep(1.0)

    print("\n" + "=" * 60)
    failed = [n for n, ok, _ in results if not ok]
    for n, ok, detail in results:
        if not ok:
            print(f"  [{FAIL}] {n} — {detail}")
    print(f"\n合计 {len(results)} 项, 通过 {len(results)-len(failed)}, 失败 {len(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
