from pathlib import Path

def get_leaf_folder(base_path):
    
    path = Path(base_path)
    
    for folder in path.rglob('*'):
        if folder.is_dir() and not any(sub.is_dir() for sub in folder.iterdir()):
            return str(folder)
        
def create_text_file(path,text):
    with open(path, 'w', encoding='utf-8') as file:
        file.write(text)
