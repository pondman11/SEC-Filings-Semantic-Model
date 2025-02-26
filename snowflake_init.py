import yaml
import snowflake.connector
import os
from src.utils.snowflake_utils import load_config, load_snowflake_config
from src.loading.load_sec_filings import upload_files

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

def extract_all_udfs(config):
    """Extracts all UDFs and their details from the YAML config."""
    udfs = {}
    try:
        for udf_name, udf_data in config["udfs"].items():
            udfs[udf_name] = {
                "source": udf_data["source"],
                "declaration": udf_data["declaration"]
            }
    except KeyError:
        return None

    return udfs

def stage_and_declare_udfs(conn,schema):
    config = load_config(schema)
    udfs = extract_all_udfs(config)
    cur = conn.cursor()
    for key in udfs.keys():
        cur.execute(f"PUT file://./src/utils/snowflake_udfs/{udfs[key]['source']} @{config['database']}.{config['schema']}.UDFS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;")
        print(f"✅ Successfully loaded UDF {key} to stage {config['database']}.{config['schema']}.UDFS...\n")
        cur.execute(f"USE SCHEMA {config['database']}.{config['schema']};")
        cur.execute(udfs[key]['declaration'])
        print(f"✅ Successfully declared UDF {key}...\n")
    
    cur.close()

def fine_tune_models(conn): 
    upload_files("data/10_k_filings",num_tickers=10,download=True,upload=True)


def main():
    """Main function to initialize the Snowflake database."""
    # Load credentials
    snowflake_config = load_snowflake_config()

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=snowflake_config["user"],
        authenticator=snowflake_config["authenticator"],
        account=snowflake_config["account"],
        warehouse=snowflake_config["warehouse"],
        role=snowflake_config["role"],
    )

    # Run database setup
    setup_snowflake_database(conn)
    stage_and_declare_udfs(conn,"raw")


    # Close connection
    conn.close()
    print("✅ Snowflake initialization script completed.")

if __name__ == "__main__":
    main()
