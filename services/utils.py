import json
from types import SimpleNamespace
import hashlib


def load_config(path: str = "config.json") -> SimpleNamespace:
    with open(path, "r") as file:
        data = json.load(file)
        return SimpleNamespace(**data)
    
def md5_hash(text: str):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def confirm_action(message: str = "Are you sure? (y/n): "):
    choice = input(message).strip().lower()
    return choice == 'y'
    
config: SimpleNamespace = load_config()