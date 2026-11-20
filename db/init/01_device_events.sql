-- 设备事件表（演示/自测用）。
-- 插入后删除一批行并 ANALYZE，使死元组估算立即可见。
CREATE TABLE IF NOT EXISTS public.device_events (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id   integer NOT NULL,
    event_type  text NOT NULL,
    payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public.device_events (device_id, event_type, payload)
SELECT (g % 50) + 1,
       (ARRAY['boot', 'telemetry', 'alarm', 'shutdown'])[(g % 4) + 1],
       jsonb_build_object('seq', g)
  FROM generate_series(1, 100000) AS g;

-- 删除一批行制造死元组（不立刻 VACUUM，留给维护 API 处理）
DELETE FROM public.device_events
 WHERE id IN (SELECT id FROM public.device_events
              WHERE event_type = 'telemetry' LIMIT 40000);

-- 演示夹具:
--   * autovacuum 保持开启（本演示正是围绕“按插入量触发 autovacuum”的门槛）；
--   * **不设置**表级 autovacuum_vacuum_insert_scale_factor —— 该表初始为
--     “继承实例默认值”。申请阈值时临时写入表级值，到期后 RESET 回继承；
--   * 把 vacuum 触发阈值调高，避免 autovacuum 在演示开始前就把上面制造的
--     4 万死元组清掉（这与插入阈值无关，只为让死元组演示可见）。
ALTER TABLE public.device_events SET (
    autovacuum_enabled = true,
    autovacuum_vacuum_threshold = 50000,
    autovacuum_vacuum_scale_factor = 1.0
);
ALTER TABLE public.device_events RESET (autovacuum_vacuum_insert_scale_factor);

ANALYZE public.device_events;

