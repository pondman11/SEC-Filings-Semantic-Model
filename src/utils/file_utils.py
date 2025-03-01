from pathlib import Path
import json
import os

def get_leaf_folder(base_path):
    
    path = Path(base_path)
    
    for folder in path.rglob('*'):
        if folder.is_dir() and not any(sub.is_dir() for sub in folder.iterdir()):
            return str(folder)
        
def create_text_file(path,text):
    with open(path, 'w', encoding='utf-8') as file:
        file.write(text)

def create_json(path,file_name,data):
    os.makedirs(path, exist_ok=True)
    with open(f'{path}\\{file_name}', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)
