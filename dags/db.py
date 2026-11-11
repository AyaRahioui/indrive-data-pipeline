import os
import psycopg2

def get_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        dbname=os.getenv("PG_DB", "indrive"),
        user=os.getenv("PG_USER", "indrive"),
        password=os.getenv("PG_PASSWORD", "indrive"),
    )

def run_sql_file(path):
    """Lit un fichier .sql et l'exécute dans UNE transaction."""
    with open(path) as f:
        sql = f.read()
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:   # commit si OK, rollback si erreur
            cur.execute(sql)
    finally:
        conn.close()