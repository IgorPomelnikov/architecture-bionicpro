Для теста нужно сделать docker compose up -d

Зайти на http://localhost:8081/dags/crm_to_clickhouse/grid под логином и паролем admin и запустить этот dag

Зайти на http://localhost:3000/, авторизоваться под test-bionic-user (пароль и логин одинаковые)

Сделать запрос отчета.

Для теста CDC см postgres/debezium-cdc-test-data.sql