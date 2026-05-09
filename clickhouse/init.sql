-- OLAP: CRM → Debezium → Kafka → KafkaEngine → MergeTree → MaterializedView → витрина.
-- Топики: crm.public.bionicpro_orders, crm.public.bionicpro_telemetry (topic.prefix crm).

-- Целевые таблицы (наполняются MV из Kafka).
CREATE TABLE IF NOT EXISTS bionicpro_orders_oltp
(
    id              String,
    user_id         String,
    bionicpro_id    String,
    bionicpro_type  String,
    purchase_date   Date,
    updated_at      DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY (id);

CREATE TABLE IF NOT EXISTS bionicpro_telemetry_oltp
(
    id               Int64,
    bionicpro_id     String,
    timestamp        DateTime64(6, 'UTC'),
    signal_strength  Float32,
    response_time_ms Float32,
    battery_level    Float32
) ENGINE = ReplacingMergeTree(timestamp)
      ORDER BY (id);

-- Витрина: SummingMergeTree — строки с одинаковым ключом суммируются при merge (инкремент с MV).
CREATE TABLE IF NOT EXISTS bionicpro_reports_mart
(
    user_id              String,
    bionicpro_id         String,
    report_date          Date,
    signal_count         UInt64,
    signal_strength_sum  Float64,
    response_time_sum    Float64,
    battery_sum          Float64
) ENGINE = SummingMergeTree()
      ORDER BY (user_id, report_date, bionicpro_id);

-- Очереди Kafka (Debezium envelope в одной строке JSON).
CREATE TABLE IF NOT EXISTS bionicpro_orders_kafka_queue
(
    raw String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'crm.public.bionicpro_orders',
    kafka_group_name = 'clickhouse_debezium_orders',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_auto_offset_reset = 'earliest';

CREATE TABLE IF NOT EXISTS bionicpro_telemetry_kafka_queue
(
    raw String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'crm.public.bionicpro_telemetry',
    kafka_group_name = 'clickhouse_debezium_telemetry',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_auto_offset_reset = 'earliest';

-- CDC → заказы (только create/update/snapshot; delete пропускаем).
CREATE MATERIALIZED VIEW IF NOT EXISTS bionicpro_orders_kafka_mv TO bionicpro_orders_oltp
AS
SELECT
    JSONExtractString(raw, 'after', 'id') AS id,
    JSONExtractString(raw, 'after', 'user_id') AS user_id,
    JSONExtractString(raw, 'after', 'bionicpro_id') AS bionicpro_id,
    JSONExtractString(raw, 'after', 'bionicpro_type') AS bionicpro_type,
    if(
        length(JSONExtractString(raw, 'after', 'purchase_date')) > 0,
        toDateOrZero(JSONExtractString(raw, 'after', 'purchase_date')),
        addDays(toDate('1970-01-01'), toInt32(JSONExtractInt(raw, 'after', 'purchase_date')))
    ) AS purchase_date,
    if(
        JSONExtractInt(raw, 'after', 'updated_at') > 1000000000000000,
        toDateTime64(JSONExtractInt(raw, 'after', 'updated_at') / 1000000, 6, 'UTC'),
        if(
            JSONExtractInt(raw, 'after', 'updated_at') > 0,
            toDateTime64(JSONExtractInt(raw, 'after', 'updated_at') / 1000, 3, 'UTC'),
            toDateTime64(now64(3), 3, 'UTC')
        )
    ) AS updated_at
FROM bionicpro_orders_kafka_queue
WHERE JSONExtractString(raw, 'op') IN ('c', 'u', 'r')
  AND position(raw, '"after"') > 0
  AND length(JSONExtractString(raw, 'after', 'id')) > 0;

-- CDC → телеметрия.
CREATE MATERIALIZED VIEW IF NOT EXISTS bionicpro_telemetry_kafka_mv TO bionicpro_telemetry_oltp
AS
SELECT
    JSONExtractInt(raw, 'after', 'id') AS id,
    JSONExtractString(raw, 'after', 'bionicpro_id') AS bionicpro_id,
    if(
        JSONExtractInt(raw, 'after', 'timestamp') > 1000000000000000,
        toDateTime64(JSONExtractInt(raw, 'after', 'timestamp') / 1000000, 6, 'UTC'),
        if(
            JSONExtractInt(raw, 'after', 'timestamp') > 0,
            toDateTime64(JSONExtractInt(raw, 'after', 'timestamp') / 1000, 3, 'UTC'),
            toDateTime64(now64(3), 3, 'UTC')
        )
    ) AS timestamp,
    toFloat32(JSONExtractFloat(raw, 'after', 'signal_strength')) AS signal_strength,
    toFloat32(JSONExtractFloat(raw, 'after', 'response_time_ms')) AS response_time_ms,
    toFloat32(JSONExtractFloat(raw, 'after', 'battery_level')) AS battery_level
FROM bionicpro_telemetry_kafka_queue
WHERE JSONExtractString(raw, 'op') IN ('c', 'u', 'r')
  AND position(raw, '"after"') > 0
  AND JSONExtractInt(raw, 'after', 'id') != 0;

-- Витрина: join телеметрии с заказами по вставкам в телеметрию.
CREATE MATERIALIZED VIEW IF NOT EXISTS bionicpro_reports_mart_mv TO bionicpro_reports_mart
AS
SELECT
    o.user_id AS user_id,
    t.bionicpro_id AS bionicpro_id,
    toDate(t.timestamp) AS report_date,
    toUInt64(1) AS signal_count,
    toFloat64(t.signal_strength) AS signal_strength_sum,
    toFloat64(t.response_time_ms) AS response_time_sum,
    toFloat64(t.battery_level) AS battery_sum
FROM bionicpro_telemetry_oltp AS t
INNER JOIN bionicpro_orders_oltp AS o ON t.bionicpro_id = o.bionicpro_id;
