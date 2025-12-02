import os
from db_connection.db_connect_pool import Database
from logger_config import logger


def load_inbound_credentials():
    inbound_name = os.getenv("INBOUND")  # "rabbit"

    db = Database("DB1")

    query = f"""
        SELECT credentials
        FROM {db.schema}.{db.table}
        WHERE inbound = %s
        LIMIT 1;
    """

    conn = db.get_conn()
    cursor = conn.cursor()
    cursor.execute(query, (inbound_name,))
    row = cursor.fetchone()
    db.put_conn(conn)

    if not row:
        raise Exception(f"No inbound credentials found for: {inbound_name}")

    creds = row[0]  # JSONB dict
    logger.info(f"Inbound credentials loaded: {creds}")
    return creds
