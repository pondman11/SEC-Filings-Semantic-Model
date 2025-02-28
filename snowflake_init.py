import os
from src.utils import snowflake_utils
from src.loading.load_sec_filings import upload_files


def main():
    """Main function to initialize the Snowflake database."""
    conn = snowflake_utils.connect()


    # Run database setup
    snowflake_utils.configure_environments(conn)
    #fine_tune_models(conn,"raw")

    # Close connection
    conn.close()
    print("✅ Snowflake initialization script completed.")

if __name__ == "__main__":
    main()
