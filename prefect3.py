from prefect import flow, task
import pymysql
import json
from pathlib import Path

@task
def read_db_config():
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, 'r') as f:
        return json.load(f)

@task
def fetch_data_from_db(host, user, password, database):
    """
    Fetches data from a MySQL database using pymysql.
    """
    conn = None
    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor  # Fetches rows as dictionaries
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
            result = cursor.fetchall()
        return result
    finally:
        if conn:
            conn.close()

@flow
def database_flow():
    # Load database configuration
    db_config = read_db_config()

    # Extract connection details from the loaded configuration
    db_host = db_config.get("host", "localhost") # Provide default if not found
    db_user = db_config.get("user")
    db_password = db_config.get("password")
    db_name = db_config.get("database")

    # Validate that essential config values are present
    if not all([db_user, db_password, db_name]):
        print("Error: Database user, password, or name not found in config.json")
        return

    data = fetch_data_from_db(db_host, db_user, db_password, db_name)
    if data:
        for row in data:
            print(row)
    else:
        print("No data fetched or an error occurred.")

if __name__ == "__main__":
    database_flow()
