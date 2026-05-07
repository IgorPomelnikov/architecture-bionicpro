-- Создание основной таблицы отчётов
CREATE TABLE IF NOT EXISTS prosthetic_reports
(
    report_date       Date,
    prosthetic_id     String,
    user_id           String,
    signal_count      UInt64,
    signal_avg        Float32,
    response_time_avg Float32,
    battery_avg       Float32,
    performance_score String
) ENGINE = Log;

-- 1. Таблица-очередь для данных из Kafka (KafkaEngine)
CREATE TABLE IF NOT EXISTS prosthetic_orders_queue
(
    id              String,
    user_id         String,
    prosthetic_id   String,
    prosthetic_type String,
    purchase_date   Date,
    updated_at      DateTime,
    op              String,
    ts_ms           DateTime
) ENGINE = Kafka
      SETTINGS kafka_broker_list = 'kafka:9092',
          kafka_topic_list = 'crm.public.prosthetic_orders',
          kafka_group_name = 'clickhouse_consumer',
          kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS prosthetic_telemetry_queue
(
    id               Int64,
    prosthetic_id    String,
    timestamp        DateTime,
    signal_strength  Float32,
    response_time_ms Float32,
    battery_level    Float32,
    op               String,
    ts_ms            DateTime
) ENGINE = Kafka
      SETTINGS kafka_broker_list = 'kafka:9092',
          kafka_topic_list = 'crm.public.prosthetic_telemetry',
          kafka_group_name = 'clickhouse_consumer',
          kafka_format = 'JSONEachRow';

-- 2. Целевые таблицы для хранения данных
CREATE TABLE IF NOT EXISTS prosthetic_orders_oltp
(
    id              String,
    user_id         String,
    prosthetic_id   String,
    prosthetic_type String,
    purchase_date   Date,
    updated_at      DateTime
) ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY (id);

CREATE TABLE IF NOT EXISTS prosthetic_telemetry_oltp
(
    id               Int64,
    prosthetic_id    String,
    timestamp        DateTime,
    signal_strength  Float32,
    response_time_ms Float32,
    battery_level    Float32
) ENGINE = MergeTree()
      ORDER BY (prosthetic_id, timestamp);

-- 3. Materialized Views для автоматической вставки из очередей
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_prosthetic_orders
    TO prosthetic_orders_oltp
AS
SELECT id,
       user_id,
       prosthetic_id,
       prosthetic_type,
       purchase_date,
       updated_at
FROM prosthetic_orders_queue
WHERE op IN ('c', 'r'); -- только create/read события

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_prosthetic_telemetry
    TO prosthetic_telemetry_oltp
AS
SELECT id,
       prosthetic_id,
       timestamp,
       signal_strength,
       response_time_ms,
       battery_level
FROM prosthetic_telemetry_queue
WHERE op IN ('c', 'r');

-- 4. Витрина для отчётов (Materialized View с агрегацией)
CREATE MATERIALIZED VIEW IF NOT EXISTS prosthetic_reports_mv
            ENGINE = SummingMergeTree()
                ORDER BY (user_id, report_date)
AS
SELECT o.user_id,
       t.prosthetic_id,
       toDate(t.timestamp)      as report_date,
       count(t.signal_strength) as signal_count,
       avg(t.signal_strength)   as signal_avg,
       avg(t.response_time_ms)  as response_time_avg,
       avg(t.battery_level)     as battery_avg,
       multiIf(
               avg(t.response_time_ms) < 100, 'excellent',
               avg(t.response_time_ms) < 200, 'good',
               'needs_calibration'
       )                        as performance_score,
       now()                    as updated_at
FROM prosthetic_telemetry_oltp t
         JOIN prosthetic_orders_oltp o ON t.prosthetic_id = o.prosthetic_id
GROUP BY o.user_id, t.prosthetic_id, report_date;