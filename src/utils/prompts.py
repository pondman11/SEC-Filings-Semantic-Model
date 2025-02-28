import yaml
from src.utils.snowflake_utils import connect
import os

class Chat: 

    def __init__(self, model='mistral-7b', conn=None):
        self.model = model
        self.conn = conn if conn != None else connect()
        
    def prompt(self, prompt, model=None): 
        cur = self.conn.cursor()
        cur.execute(f"""SELECT TRIM(snowflake.cortex.COMPLETE (
                        '{model if model != None else self.model}',
                        '{self.__get_prompt(prompt)}'
                        ), '\n') AS completion;""")
        completion = cur.fetchone()
        cur.close()
        return completion[0]
    
 
    def __get_prompt(self, prompt): 
        config_path = os.path.join(os.path.dirname(__file__), "..","..","config","prompts", "prompts.yml")
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        return config[prompt]