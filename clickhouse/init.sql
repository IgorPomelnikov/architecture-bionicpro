-- OLAP-слой: OLTp-таблицы и витрина наполняются batch-ETL из Apache Airflow (CRM Postgres).
-- Поток: crm_db → Airflow DAG crm_to_clickhouse → ClickHouse.

CREATE TABLE IF NOT EXISTS bionicpro_orders_oltp
(
    id              String,
    user_id         String,
    bionicpro_id    String,
    bionicpro_type  String,
    purchase_date   Date,
    updated_at      DateTime
) ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY (id);

CREATE TABLE IF NOT EXISTS bionicpro_telemetry_oltp
(
    id               Int64,
    bionicpro_id     String,
    timestamp        DateTime,
    signal_strength  Float32,
    response_time_ms Float32,
    battery_level    Float32
) ENGINE = MergeTree()
      ORDER BY (bionicpro_id, timestamp);

-- Витрина для report-service; пересобирается в конце DAG (TRUNCATE + INSERT … SELECT).
CREATE TABLE IF NOT EXISTS bionicpro_reports_mart
(
    user_id            String,
    bionicpro_id       String,
    report_date        Date,
    signal_count       UInt64,
    signal_avg         Float32,
    response_time_avg  Float32,
    battery_avg        Float32,
    performance_score  String
) ENGINE = MergeTree()
      ORDER BY (user_id, report_date, bionicpro_id);
