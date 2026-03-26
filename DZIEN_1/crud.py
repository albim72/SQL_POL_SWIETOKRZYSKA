from typing import List,Dict,Any,Optional

def insert(collection: List[Dict[str,Any]],document: Dict[str,Any]) -> None:
    collection.append(document)
    
def find_all(collection: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    return collection

def find_one(collection: List[Dict[str,Any]],key: str, value: Any) -> Optional[Dict[str,Any]]:
    for doc in collection:
        if doc.get(key) == value:
            return doc
    return None

def update_one(collection: List[Dict[str,Any]],key: str, value: Any, updates: Dict[str,Any]) -> bool:
    for doc in collection:
        if doc.get(key) == value:
            doc.update(updates)
            return True
    return False

def delete_one(collection: List[Dict[str,Any]],key: str, value: Any) -> bool:
    for i, doc in enumerate(collection):
        if doc.get(key) == value:
            del collection[i] 
            return True
    return False
    
