import yaml
import os
import snowflake.connector

def load_config(env):
    """Load Snowflake credentials from YAML configuration file."""
    config_path = os.path.join(os.path.dirname(__file__), "..","..","config", "snowflake_config.yml")
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config[env]

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

def get_schema_config(schema):
    return load_config(schema)

def load_snowflake_config(): 
    return load_config("snowflake")

def get_stage(schema): 
    config = get_schema_config(schema)
    return f'{config["database"]}.{config["schema"]}.{config["stage"]}'