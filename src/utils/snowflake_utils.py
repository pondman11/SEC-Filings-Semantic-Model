import yaml
import snowflake.connector

def load_snowflake_config():
    """Load Snowflake credentials from YAML configuration file."""
    with open("config/snowflake_config.yml", "r") as file:
        config = yaml.safe_load(file)
    return config["snowflake"]

def check_snowflake_database():
    """Check if the Snowflake database exists."""
    config = load_snowflake_config()
    
    conn = snowflake.connector.connect(
        user=config["user"],
        password=config["password"],
        account=config["account"],
        warehouse=config["warehouse"],
        role=config["role"],
    )

    cur = conn.cursor()
    cur.execute("SHOW DATABASES LIKE 'SEC_FILINGS_DB'")

    database_exists = cur.fetchone() is not None
    cur.close()
    conn.close()

    return database_exists
