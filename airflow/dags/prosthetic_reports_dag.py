from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import psycopg2

default_args = {
    'owner': 'bionicpro',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def _get_settings():
    """Настройки из env с дефолтами для локального окружения."""
    return {
        'pg_host': os.getenv('PROSTHETIC_PG_HOST', 'localhost'),
        'pg_port': int(os.getenv('PROSTHETIC_PG_PORT', '5433')),
        'pg_db': os.getenv('PROSTHETIC_PG_DB', 'keycloak_db'),
        'pg_user': os.getenv('PROSTHETIC_PG_USER', 'keycloak_user'),
        'pg_password': os.getenv('PROSTHETIC_PG_PASSWORD', 'keycloak_password'),
        'clickhouse_url': os.getenv('PROSTHETIC_CLICKHOUSE_URL', 'http://localhost:8123'),
        'request_timeout_sec': int(os.getenv('PROSTHETIC_HTTP_TIMEOUT_SEC', '10')),
    }

def run_etl():
    """Основной ETL процесс"""
    settings = _get_settings()

    conn = psycopg2.connect(
        host=settings['pg_host'],
        port=settings['pg_port'],
        database=settings['pg_db'],
        user=settings['pg_user'],
        password=settings['pg_password']
    )
    cur = conn.cursor()

    # Получаем CRM данные
    cur.execute("SELECT prosthetic_id, user_id FROM prosthetic_orders")
    crm = {row[0]: row[1] for row in cur.fetchall()}
    print(f"CRM records: {len(crm)}")

    # Получаем телеметрию
    cur.execute("SELECT prosthetic_id, signal_strength, response_time_ms, battery_level FROM prosthetic_telemetry")
    telemetry = cur.fetchall()
    print(f"Telemetry records: {len(telemetry)}")

    if len(telemetry) == 0:
        print("No telemetry data")
        cur.close()
        conn.close()
        return 0

    # Агрегация
    agg = {}
    for pid, signal, response, battery in telemetry:
        if pid not in agg:
            agg[pid] = {'count': 0, 'signal_sum': 0, 'response_sum': 0, 'battery_sum': 0}
        agg[pid]['count'] += 1
        agg[pid]['signal_sum'] += signal
        agg[pid]['response_sum'] += response
        agg[pid]['battery_sum'] += battery

    # Вставляем данные в ClickHouse
    inserted = 0
    for pid, data in agg.items():
        signal_avg = data['signal_sum'] / data['count']
        response_avg = data['response_sum'] / data['count']
        battery_avg = data['battery_sum'] / data['count']

        if response_avg < 100:
            score = 'excellent'
        elif response_avg < 200:
            score = 'good'
        else:
            score = 'needs_calibration'

        user_id = crm.get(pid, 'unknown')

        sql = f"""
        INSERT INTO prosthetic_reports VALUES (
            today(),
            '{pid}',
            '{user_id}',
            {data['count']},
            {signal_avg},
            {response_avg},
            {battery_avg},
            '{score}'
        )
        """
        try:
            resp = requests.post(
                settings['clickhouse_url'],
                data=sql,
                timeout=settings['request_timeout_sec'],
            )
        except requests.RequestException as exc:
            print(f"Failed {pid}: request error - {exc}")
            continue

        if resp.status_code == 200:
            inserted += 1
            print(f"Inserted {pid}: {score}")
        else:
            print(f"Failed {pid}: {resp.status_code} - {resp.text[:200]}")

    cur.close()
    conn.close()
    print(f"ETL completed: {inserted} records")
    return inserted

with DAG(
        dag_id='prosthetic_reports_etl',
        default_args=default_args,
        description='ETL для витрины отчётов протезов',
        schedule='*/5 * * * *',
        catchup=False,
) as dag:

    etl = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl,
    )