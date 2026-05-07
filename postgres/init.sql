CREATE TABLE IF NOT EXISTS prosthetic_orders
(
    id              VARCHAR(50) PRIMARY KEY,
    user_id         VARCHAR(50),
    prosthetic_id   VARCHAR(50),
    prosthetic_type VARCHAR(100),
    purchase_date   DATE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS prosthetic_telemetry
(
    id               SERIAL PRIMARY KEY,
    prosthetic_id    VARCHAR(50),
    timestamp        TIMESTAMP DEFAULT NOW(),
    signal_strength  FLOAT,
    response_time_ms FLOAT,
    battery_level    FLOAT
);

INSERT INTO prosthetic_orders (id, user_id, prosthetic_id, prosthetic_type, purchase_date)
SELECT *
FROM (VALUES ('1', 'john.doe', 'prosthetic_1', 'bionic_hand', '2024-01-01'::DATE),
             ('2', 'jane.smith', 'prosthetic_2', 'bionic_arm', '2024-01-02'::DATE),
             ('3', 'alex.johnson', 'prosthetic_3', 'bionic_leg', '2024-01-03'::DATE))
         AS v(id, user_id, prosthetic_id, prosthetic_type, purchase_date)
WHERE NOT EXISTS (SELECT 1 FROM prosthetic_orders LIMIT 1);

INSERT INTO prosthetic_telemetry (prosthetic_id, signal_strength, response_time_ms, battery_level)
SELECT *
FROM (VALUES ('prosthetic_1', 85, 95, 75),
             ('prosthetic_1', 82, 102, 73),
             ('prosthetic_1', 78, 110, 70),
             ('prosthetic_2', 78, 150, 68),
             ('prosthetic_2', 75, 165, 65),
             ('prosthetic_3', 92, 75, 82),
             ('prosthetic_3', 90, 78, 80))
         AS v(prosthetic_id, signal_strength, response_time_ms, battery_level)
WHERE NOT EXISTS (SELECT 1 FROM prosthetic_telemetry LIMIT 1);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE USER debezium WITH PASSWORD 'debezium';
    END IF;
END $$;

ALTER USER debezium WITH REPLICATION;
GRANT CONNECT ON DATABASE keycloak_db TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

DROP PUBLICATION IF EXISTS dbz_publication;
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
