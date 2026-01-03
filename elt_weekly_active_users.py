from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging


def _get_cursor():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn, conn.cursor()


@task
def run_ctas(schema, table, select_sql, primary_key=None):
    conn, cur = _get_cursor()
    
    logging.info(table)
    logging.info(select_sql)

    try:
        sql = f"CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # do primary key uniqueness check
        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1"""
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")

        dup_check_sql = f"""
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT USERID, SESSIONID, CHANNEL, TS) AS distinct_rows
            FROM {schema}.temp_{table}
        """
        logging.info(f"Duplicate check SQL:\n{dup_check_sql}")
        cur.execute(dup_check_sql)
        total_rows, distinct_rows = cur.fetchone()
        if total_rows > distinct_rows:
            raise Exception(f"Duplicate rows detected: {total_rows - distinct_rows} duplicates")    


        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} AS
            SELECT * FROM {schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        swap_sql = f"""ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};"""
        cur.execute(swap_sql)
    except Exception as e:
        raise
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id = 'elt_snowflake_wau',
    start_date = datetime(2025,10,25),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:

    schema = "USER_DB_LIZARD.analytics"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts
        FROM USER_DB_LIZARD.raw.user_session_channel u
        JOIN USER_DB_LIZARD.raw.session_timestamp s ON u.sessionId=s.sessionId
    """

    run_ctas(schema, table, select_sql, primary_key='sessionId')
