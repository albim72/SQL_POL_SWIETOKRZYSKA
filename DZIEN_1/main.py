from sample_data import users
from crud import insert, find_all, find_one, update_one, delete_one
from storage import save_to_json, load_from_json
from utils import print_collection, build_index

def main():
    collection = users.copy()
    print_collection("users", collection)

    new_user  = {
        "user_id": 3,
        "name": "Ewa Zielińska",
        "email": "ewa@example.com",
        "addresses": [
            {"city": "Lublin", "street": "Polna 7"}
        ],
        "interests": ["ethics", "databases", "books"]
    }
    print("\nINSERT")
    insert(collection, new_user)
    print_collection("po dodaniu użykownika", collection)

    print("\nFIND ONE")
    user = find_one(collection,"user_id",2)
    print(user)

    print("\nUPDATE")
    upadated = update_one(collection,"user_id",1,{"email":"anna.kowalska@exaple.com"})
    print("Zaktualizowano",upadated)
    print_collection("po aktualizacji", collection)

    print("\nDELETE")
    deleted = delete_one(collection,"user_id",2)
    print("usunięto",deleted)
    print_collection("po usunięciu", collection)

    print("\nSAVE TO JSON")
    save_to_json("data/users.json",collection)
    print("plik zapisany")

    print("\nLOAD FROM JSON")
    loaded = load_from_json("data/users.json")
    print_collection("wczytane dane",loaded)

# if __name__ == "__main__":
if  "__main__"==__name__:
    main()
