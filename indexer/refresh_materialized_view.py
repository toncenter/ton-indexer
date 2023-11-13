import psycopg2
import os
import time
from loguru import logger


# Connect to the database
dsn = os.getenv('TON_INDEXER_PG_DSN')
if not dsn:
    raise Exception('TON_INDEXER_PG_DSN environment variable is not defined')
dsn = dsn.replace('+asyncpg', '')

while True:
    try:
        # Connect to the database using DSN
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                # Check if the materialized view exists
                cur.execute("SELECT EXISTS (SELECT FROM pg_matviews WHERE matviewname = 'latest_account_balances');")
                view_exists = cur.fetchone()[0]

                if not view_exists:
                    # Create materialized view
                    cur.execute("""
                    CREATE MATERIALIZED VIEW latest_account_balances AS
                    SELECT a.account, a.balance
                    FROM account_states a
                    INNER JOIN (
                        SELECT account, MAX(timestamp) as max_timestamp
                        FROM account_states
                        WHERE balance > 0
                        GROUP BY account
                    ) b ON a.account = b.account AND a.timestamp = b.max_timestamp;
                    """)
                    logger.info("Materialized view created")

                    # Check if the index exists
                    cur.execute("SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_latest_account_balances_balance');")
                    index_exists = cur.fetchone()[0]

                    if not index_exists:
                        # Create index
                        cur.execute("CREATE INDEX idx_latest_account_balances_balance ON latest_account_balances(balance DESC);")
                        logger.info("Index created")
                else:        
                    # Refresh materialized view
                    start = time.time()
                    cur.execute('REFRESH MATERIALIZED VIEW latest_account_balances;')
                    logger.info(f"Materialized view refreshed in {(time.time() - start):.2f} seconds")

        time.sleep(180)
    except Exception as e:
        logger.info(f"Error: {e}")
        time.sleep(5)
        continue
