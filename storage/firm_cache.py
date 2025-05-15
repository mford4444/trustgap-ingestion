import json
import os

CACHE_FILE = "storage/firm_cache.json"

def load_previous_firms():
    if not os.path.exists(CACHE_FILE):
        return {}

    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        print("⚠️ Warning: Corrupted firm_cache.json — ignoring and rebuilding.")
        return {}

def save_current_firms(firms):
    cache = {str(f["crd_number"]): f["filing_date"].strftime("%Y-%m-%d") for f in firms}

    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)
