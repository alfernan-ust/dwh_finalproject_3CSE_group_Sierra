import psycopg2
import os
import sys

DB_HOST = "postgres"
DB_NAME = "kestra"
DB_USER = "kestra"
DB_PASS = "k3str4"

SQL_FILE_PATH = "/opt/kestra/workspace/sql/add_unique_constraints.sql"

def main():
    print(f"[MIGRATION] Adding unique constraints to existing tables")
    print(f"[MIGRATION] Looking for SQL file at: {SQL_FILE_PATH}")

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

        # Execute the migration
        cur.execute(sql)
        print("[SUCCESS] Unique constraints added successfully.")

    except psycopg2.errors.UniqueViolation as e:
        print(f"[ERROR] Duplicate values found in table. Cannot add unique constraint.")
        print(f"[ERROR] Details: {e}")
        print(f"[INFO] You may need to clean up duplicate data first.")
        sys.exit(1)

    except Exception as e:
        print(f"[DATABASE ERROR] {e}")
        sys.exit(1)

    finally:
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.close()

if __name__ == "__main__":
    main()
