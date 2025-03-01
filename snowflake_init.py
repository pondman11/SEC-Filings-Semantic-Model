import os
from src.utils import snowflake_utils
from src.utils.web_retrieval_utils import get_CIKs
from src.utils.file_utils import create_json


def main():
    """Main function to initialize the Snowflake database."""
    conn = snowflake_utils.connect()

    # Run database setup
    #snowflake_utils.configure_environments(conn)
    #company_data_path = os.path.join(os.path.dirname(__file__),"data","companies")
    #create_json(company_data_path,"CIKs.json", get_CIKs())
    schema = snowflake_utils.get_schema_config("raw")
    #snowflake_utils.clear_stage(conn,schema,"COMPANIES")
    #snowflake_utils.load_to_stage(conn,schema,"COMPANIES",company_data_path,"json")
    snowflake_utils.run_sql_files(conn,schema)
    # Close connection
    conn.close()
    print("✅ Snowflake initialization script completed.")

if __name__ == "__main__":
    main()
