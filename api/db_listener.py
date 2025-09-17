# api/db_listener.py
import sys
import os
import threading
from pymongo import MongoClient
from dotenv import load_dotenv

# Add ml folder to path to import train_model
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../ml")))
import train_model

# Load env
load_dotenv()
MONGO_URI = os.getenv("MONGODB_CONNECTION_STRING")
DB_NAME = "kerala_health_system"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
patients_col = db.patients
disease_col = db.disease_cases

def watch_collection(collection, name):
    print(f"👀 Listening for changes in {name}...")
    with collection.watch() as stream:
        for change in stream:
            print(f"\n⚡ Change detected in {name}: {change['operationType']}")
            # Regenerate district JSON on any change
            try:
                train_model.regenerate_district_json()
            except Exception as e:
                print(f"❌ Error regenerating district JSON: {e}")

def start_listener():
    # Run each watcher in its own thread
    threading.Thread(target=watch_collection, args=(patients_col, "patients"), daemon=True).start()
    threading.Thread(target=watch_collection, args=(disease_col, "disease_cases"), daemon=True).start()
    print("✅ DB listener started. Press Ctrl+C to stop.")

if __name__ == "__main__":
    start_listener()
    # Keep the main thread alive
    import time
    while True:
        time.sleep(10)
