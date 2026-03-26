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

# if __name__ == "__main__":
if  "__main__"==__name__:
    main()
