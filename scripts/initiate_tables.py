import psycopg2
import os
import sys

# Configuration matches docker-compose service names and mapped paths
DB_HOST = "postgres"
DB_NAME = "kestra"
DB_USER = "kestra"
DB_PASS = "k3str4"
SQL_FILE_PATH = "/sql/create_tables.sql"

def main():
    print(f"Checking for SQL file at: {SQL_FILE_PATH}")
    
    if not os.path.exists(SQL_FILE_PATH):
        print(f"ERROR: SQL file not found at {SQL_FILE_PATH}")
        print("Please check your docker-compose volumes mapping for '/sql'.")
        sys.exit(1)

    try:
        # Connect to Database
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Read SQL content
        print("Reading SQL file...")
        with open(SQL_FILE_PATH, 'r') as f:
            sql_script = f.read()

        # Execute
        print("Executing DDL statements...")
        cur.execute(sql_script)
        
        print("Tables initialized successfully.")
        
    except Exception as e:
        print(f"Database Error: {e}")
        sys.exit(1)
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    main()