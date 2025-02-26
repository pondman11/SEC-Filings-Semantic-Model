from snowflake.snowpark.files import SnowflakeFile

class read_file:
    def process(self,stage_path): 

        with SnowflakeFile.open(stage_path, 'r') as f:
            content = f.read()
        
        return content