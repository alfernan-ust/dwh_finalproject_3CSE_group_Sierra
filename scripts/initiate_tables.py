import psycopg2
import os
import sys

DB_HOST = "postgres"
DB_NAME = "kestra"
DB_USER = "kestra"
DB_PASS = "k3str4"

SQL_FILE_PATH = "/opt/kestra/workspace/sql/create_tables.sql"

def main():
    print(f"[INIT] Looking for SQL file at: {SQL_FILE_PATH}")

    if not os.path.isfile(SQL_FILE_PATH):
        print(f"[ERROR] SQL file not found: {SQL_FILE_PATH}")
        sys.exit(1)

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = True
        cur = conn.cursor()

        with open(SQL_FILE_PATH, "r", encoding="utf-8") as f:
            sql = f.read()

        cur.execute(sql)
        print("[SUCCESS] Tables initialized.")

    except Exception as e:
        print("[DATABASE ERROR]", e)
        sys.exit(1)

    finally:
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.close()

if __name__ == "__main__":
    main()
