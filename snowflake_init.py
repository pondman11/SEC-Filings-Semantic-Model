import yaml
import snowflake.connector
import os

def load_snowflake_config():
    """Load Snowflake credentials from YAML configuration file."""
    with open("config/snowflake_config.yml", "r") as file:
        config = yaml.safe_load(file)
    return config["snowflake"]

def setup_snowflake_database(conn):
    """Execute the Snowflake database setup script."""
    sql_file_path = "sql/create_snowflake_db.sql"

    if not os.path.exists(sql_file_path):
        print(f"SQL script not found: {sql_file_path}")
        return

    with open(sql_file_path, "r") as f:
        sql_script = f.read()

    cur = conn.cursor()
    for query in sql_script.split(";"):
        if query.strip():
            cur.execute(query)
    cur.close()

    print("✅ Snowflake database setup completed successfully!")

def main():
    """Main function to initialize the Snowflake database."""
    # Load credentials
    snowflake_config = load_snowflake_config()

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=snowflake_config["user"],
        password=snowflake_config["password"],
        account=snowflake_config["account"],
        warehouse=snowflake_config["warehouse"],
        role=snowflake_config["role"],
    )

    # Run database setup
    setup_snowflake_database(conn)

    # Close connection
    conn.close()
    print("✅ Snowflake initialization script completed.")

if __name__ == "__main__":
    main()
