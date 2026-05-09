-- CRM OLTP (отдельно от Keycloak). Публикация и пользователь debezium — для CDC → Kafka → ClickHouse (KafkaEngine).
CREATE TABLE IF NOT EXISTS bionicpro_orders
(
    id              VARCHAR(50) PRIMARY KEY,
    user_id         VARCHAR(50),
    bionicpro_id    VARCHAR(50),
    bionicpro_type  VARCHAR(100),
    purchase_date   DATE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bionicpro_telemetry
(
    id               SERIAL PRIMARY KEY,
    bionicpro_id     VARCHAR(50),
    timestamp        TIMESTAMP DEFAULT NOW(),
    signal_strength  FLOAT,
    response_time_ms FLOAT,
    battery_level    FLOAT
);

INSERT INTO bionicpro_orders (id, user_id, bionicpro_id, bionicpro_type, purchase_date)
SELECT *
FROM (VALUES ('1', 'john.doe', 'bionicpro_1', 'bionic_hand', '2024-01-01'::DATE),
             ('2', 'jane.smith', 'bionicpro_2', 'bionic_arm', '2024-01-02'::DATE),
             ('3', 'alex.johnson', 'bionicpro_3', 'bionic_leg', '2024-01-03'::DATE))
         AS v(id, user_id, bionicpro_id, bionicpro_type, purchase_date)
WHERE NOT EXISTS (SELECT 1 FROM bionicpro_orders LIMIT 1);

INSERT INTO bionicpro_telemetry (bionicpro_id, signal_strength, response_time_ms, battery_level)
SELECT *
FROM (VALUES ('bionicpro_1', 85, 95, 75),
             ('bionicpro_1', 82, 102, 73),
             ('bionicpro_1', 78, 110, 70),
             ('bionicpro_2', 78, 150, 68),
             ('bionicpro_2', 75, 165, 65),
             ('bionicpro_3', 92, 75, 82),
             ('bionicpro_3', 90, 78, 80))
         AS v(bionicpro_id, signal_strength, response_time_ms, battery_level)
WHERE NOT EXISTS (SELECT 1 FROM bionicpro_telemetry LIMIT 1);

INSERT INTO bionicpro_orders (id, user_id, bionicpro_id, bionicpro_type, purchase_date)
SELECT *
FROM (VALUES ('4', 'igor', 'bionicpro_igor_1', 'bionic_hand', '2025-05-01'::DATE))
         AS v(id, user_id, bionicpro_id, bionicpro_type, purchase_date)
WHERE NOT EXISTS (SELECT 1 FROM bionicpro_orders WHERE id = '4');

INSERT INTO bionicpro_telemetry (bionicpro_id, signal_strength, response_time_ms, battery_level)
SELECT *
FROM (VALUES ('bionicpro_igor_1', 88, 92, 78),
             ('bionicpro_igor_1', 86, 98, 76),
             ('bionicpro_igor_1', 84, 105, 74))
         AS v(bionicpro_id, signal_strength, response_time_ms, battery_level)
WHERE NOT EXISTS (SELECT 1 FROM bionicpro_telemetry WHERE bionicpro_id = 'bionicpro_igor_1' LIMIT 1);

-- Keycloak: username test-bionic-user (realm reports-realm)
INSERT INTO bionicpro_orders (id, user_id, bionicpro_id, bionicpro_type, purchase_date)
SELECT *
FROM (VALUES ('5', 'test-bionic-user', 'bionicpro_test_bionic_1', 'bionic_hand', '2025-06-01'::DATE))
         AS v(id, user_id, bionicpro_id, bionicpro_type, purchase_date)
WHERE NOT EXISTS (SELECT 1 FROM bionicpro_orders WHERE id = '5');

INSERT INTO bionicpro_telemetry (bionicpro_id, signal_strength, response_time_ms, battery_level)
SELECT *
FROM (VALUES ('bionicpro_test_bionic_1', 90, 88, 80),
             ('bionicpro_test_bionic_1', 87, 95, 77),
             ('bionicpro_test_bionic_1', 85, 108, 74),
             ('bionicpro_test_bionic_1', 83, 115, 72))
         AS v(bionicpro_id, signal_strength, response_time_ms, battery_level)
WHERE NOT EXISTS (SELECT 1 FROM bionicpro_telemetry WHERE bionicpro_id = 'bionicpro_test_bionic_1' LIMIT 1);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE USER debezium WITH PASSWORD 'debezium';
    END IF;
END $$;

ALTER USER debezium WITH REPLICATION;
GRANT CONNECT ON DATABASE crm TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

DROP PUBLICATION IF EXISTS dbz_publication;
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
