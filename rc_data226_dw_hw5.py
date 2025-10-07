# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import requests


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


@task
def fetch_last_90d_price(symbol: str):
    vantage_api_key = Variable.get("vantage_api_key")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "Time Series (Daily)" not in data:
        note = data.get("Note") or data.get("Error Message") or str(data)[:300]
        raise RuntimeError(f"Alpha Vantage error/throttle: {note}")

    results = []
    for d in data["Time Series (Daily)"]:
        stock_info = data["Time Series (Daily)"][d].copy()
        stock_info["date"] = d                   # YYYY-MM-DD
        stock_info["symbol"] = symbol
        results.append(stock_info)

    results.sort(key=lambda x: x["date"], reverse=True)
    return results[:90]


@task
def load_full_refresh(records):
    target_db = Variable.get("target_db", default_var=None)
    target_schema = Variable.get("target_schema", default_var="RAW")
    target_table_name = Variable.get("target_table", default_var="STOCK_PRICE")

    target_table = (
        f"{target_db}.{target_schema}.{target_table_name}"
        if target_db else f"{target_schema}.{target_table_name}"
    )

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                OPEN FLOAT,
                HIGH FLOAT,
                LOW  FLOAT,
                CLOSE FLOAT,
                T_VOLUME BIGINT,
                T_DATE   DATE,
                SYMBOL   VARCHAR,
                PRIMARY KEY (T_DATE, SYMBOL)
            );
            """
        )

        # full refresh
        cur.execute(f"DELETE FROM {target_table}")

        insert_sql = f"""
            INSERT INTO {target_table}
                (OPEN, HIGH, LOW, CLOSE, T_VOLUME, T_DATE, SYMBOL)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for r in records:
            tup = (
                r["1. open"],
                r["2. high"],
                r["3. low"],
                r["4. close"],
                r["5. volume"],
                r["date"],
                r["symbol"],
            )
            cur.execute(insert_sql, tup)

        cur.execute("COMMIT;")

        cur.execute(f"SELECT COUNT(*) FROM {target_table}")
        return {"inserted_rows": cur.fetchone()[0], "table": target_table}
	
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e


with DAG(
    dag_id="rc_data226_dw_hw5",
    start_date=datetime(2025, 10, 1),
    schedule="17 1 * * *",   # 01:17 daily
    catchup=False,
    tags=["ETL", "RC_hw5"],
) as dag:

    symbol = Variable.get("stock_symbol", default_var="IBM")

    price_list = fetch_last_90d_price(symbol)
    load_task = load_full_refresh(price_list)

    price_list >> load_task
