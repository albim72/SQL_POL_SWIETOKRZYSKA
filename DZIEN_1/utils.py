from typing import List,Dict,Any

def print_collection(title: str, collection: List[Dict[str,Any]]) -> None:
    print(f"\n{title}:")
    if not collection:
        print("brak danych")
        
    for doc in collection:
        print(f"  {doc}")
        
def build_index(collection: List[Dict[str,Any]],key: str) -> Dict[Any, Dict[str,Any]]:
    index = {}
    for doc in collection:
        if key in doc:
            index[doc[key]] = doc
    return index
