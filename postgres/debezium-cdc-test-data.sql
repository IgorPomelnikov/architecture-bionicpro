-- Набор изменений в CRM для проверки Debezium → Kafka.
-- Запуск (пример): из хоста
--   docker compose exec -T crm_db psql -U crm_user -d crm -f - < postgres/debezium-cdc-test-data.sql
-- или внутри контейнера:
--   psql -U crm_user -d crm -f /path/to/debezium-cdc-test-data.sql
--
-- Коннектор должен быть зарегистрирован; таблицы: public.bionicpro_orders, public.bionicpro_telemetry.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1) INSERT заказа → в Kafka ожидается событие op = "c" (create)
-- ---------------------------------------------------------------------------
INSERT INTO bionicpro_orders (id, user_id, bionicpro_id, bionicpro_type, purchase_date, updated_at)
VALUES (
    'cdc-demo-order',
    'cdc-tester',
    'bionicpro_cdc_demo_1',
    'bionic_hand',
    CURRENT_DATE,
    NOW()
)
ON CONFLICT (id) DO UPDATE SET
    user_id         = EXCLUDED.user_id,
    bionicpro_id    = EXCLUDED.bionicpro_id,
    bionicpro_type  = EXCLUDED.bionicpro_type,
    purchase_date   = EXCLUDED.purchase_date,
    updated_at      = NOW();

-- ---------------------------------------------------------------------------
-- 2) INSERT телеметрии (несколько строк) → несколько "c"
-- ---------------------------------------------------------------------------
INSERT INTO bionicpro_telemetry (bionicpro_id, timestamp, signal_strength, response_time_ms, battery_level)
VALUES
    ('bionicpro_cdc_demo_1', NOW() - INTERVAL '3 minutes', 80, 120, 70),
    ('bionicpro_cdc_demo_1', NOW() - INTERVAL '2 minutes', 82, 115, 71),
    ('bionicpro_cdc_demo_1', NOW() - INTERVAL '1 minute',  84, 110, 72);

-- ---------------------------------------------------------------------------
-- 3) UPDATE заказа → op = "u" (поле updated_at тоже меняется)
-- ---------------------------------------------------------------------------
UPDATE bionicpro_orders
SET bionicpro_type = 'bionic_hand_research',
    updated_at     = NOW()
WHERE id = 'cdc-demo-order';

-- ---------------------------------------------------------------------------
-- 4) UPDATE одной строки телеметрии → "u"
-- ---------------------------------------------------------------------------
UPDATE bionicpro_telemetry
SET signal_strength  = 95,
    response_time_ms = 90,
    battery_level    = 85
WHERE bionicpro_id = 'bionicpro_cdc_demo_1'
  AND id = (
      SELECT id
      FROM bionicpro_telemetry
      WHERE bionicpro_id = 'bionicpro_cdc_demo_1'
      ORDER BY id ASC
      LIMIT 1
  );

-- ---------------------------------------------------------------------------
-- 5) INSERT ещё одной точки телеметрии → снова "c"
-- ---------------------------------------------------------------------------
INSERT INTO bionicpro_telemetry (bionicpro_id, timestamp, signal_strength, response_time_ms, battery_level)
VALUES ('bionicpro_cdc_demo_1', NOW(), 88, 100, 80);

-- ---------------------------------------------------------------------------
-- 6) DELETE одной строки телеметрии → op = "d" (в envelope будет "before")
--    Удаляем последнюю вставленную строку для этого устройства (макс. id).
-- ---------------------------------------------------------------------------
DELETE FROM bionicpro_telemetry
WHERE id = (
    SELECT id
    FROM bionicpro_telemetry
    WHERE bionicpro_id = 'bionicpro_cdc_demo_1'
    ORDER BY id DESC
    LIMIT 1
);

COMMIT;

-- Проверка в Postgres:
-- SELECT * FROM bionicpro_orders WHERE id = 'cdc-demo-order';
-- SELECT * FROM bionicpro_telemetry WHERE bionicpro_id = 'bionicpro_cdc_demo_1' ORDER BY id;
