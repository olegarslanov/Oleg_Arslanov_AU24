import pytest
import psycopg2
import yaml

@pytest.fixture(scope="session")
def db_connection():
    """Connection to DB and auto close after session"""
    with open("config_db.yaml", 'r') as f:
        config = yaml.safe_load(f)["database"]

    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            dbname=config["dbname"]
        )
        print("✅ Connected to DB")
        yield conn
    except Exception as e:
        pytest.fail(f"❌ Failed to connect to DB: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("🔒 Connection closed")

