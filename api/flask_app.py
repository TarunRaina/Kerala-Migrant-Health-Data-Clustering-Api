# api/flask_app.py
import sys
import os
import threading
import time
from pymongo import MongoClient
from dotenv import load_dotenv

# Add ml folder to path to import train_model
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../ml")))
import train_model

from flask import Flask, request, jsonify
from flask_cors import CORS
import json

# ------------------------
# Load env & DB
# ------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGODB_CONNECTION_STRING")
DB_NAME = "kerala_health_system"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
patients_col = db.patients
disease_col = db.disease_cases

# ------------------------
# Flask Setup
# ------------------------
app = Flask(__name__)
CORS(app)
DISTRICT_DATA_FILE = os.path.join(os.path.dirname(__file__), '../district_data/district_data.json')

def load_district_data():
    with open(DISTRICT_DATA_FILE, 'r') as f:
        return json.load(f)

# ------------------------
# DB Listener
# ------------------------
def watch_collection(collection, name):
    print(f"👀 Listening for changes in {name}...")
    with collection.watch() as stream:
        for change in stream:
            print(f"\n⚡ Change detected in {name}: {change['operationType']}")
            try:
                train_model.regenerate_district_json()
                print(f"✅ District JSON updated due to change in {name}")
            except Exception as e:
                print(f"❌ Error regenerating district JSON: {e}")

def start_listener():
    threading.Thread(target=watch_collection, args=(patients_col, "patients"), daemon=True).start()
    threading.Thread(target=watch_collection, args=(disease_col, "disease_cases"), daemon=True).start()
    print("✅ DB listener threads started.")

# ------------------------
# Prepopulate JSON on startup
# ------------------------
try:
    print("⚡ Regenerating district JSON on startup...")
    train_model.regenerate_district_json()
    print("✅ District JSON ready.")
except Exception as e:
    print(f"❌ Failed to regenerate district JSON on startup: {e}")

# Start listener threads
start_listener()

# ------------------------
# Routes
# ------------------------
@app.route('/')
def home():
    return jsonify({"message": "API alive. No sugar-coating, only raw power."})

@app.route('/district_info', methods=['GET'])
def get_district_info():
    district = request.args.get('district')
    if not district:
        return jsonify({"error": "Missing 'district' query parameter"}), 400

    district_data = load_district_data()
    if district not in district_data:
        return jsonify({"error": f"No data found for district '{district}'"}), 404

    return jsonify(district_data[district]), 200

# ------------------------
# Run Server
# ------------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
