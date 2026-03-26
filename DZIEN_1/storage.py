import json
from pathlib import Path
from typing import List,Dict,Any

def save_to_json(path: str, collection : List[Dict[str,Any]]) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(collection, file, indent=4, ensure_ascii=False)
        
def load_from_json(path: str) -> List[Dict[str,Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return []

    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)
