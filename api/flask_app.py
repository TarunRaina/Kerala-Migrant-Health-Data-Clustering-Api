from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import os

# Setup
app = Flask(__name__)
CORS(app)
# Path to precomputed district data
DISTRICT_DATA_FILE = os.path.join(os.path.dirname(__file__), '../district_data/district_data.json')

def load_district_data():
    with open(DISTRICT_DATA_FILE, 'r') as f:
        return json.load(f)

# 🧱 Basic Routes

@app.route('/')
def home():
    return jsonify({"message": "API alive. No sugar-coating, only raw power."})

# 🏥 Get Disease Summary by District
@app.route('/district_info', methods=['GET'])
def get_district_info():
    district = request.args.get('district')
    if not district:
        return jsonify({"error": "Missing 'district' query parameter"}), 400

    district_data = load_district_data()
    if district not in district_data:
        return jsonify({"error": f"No data found for district '{district}'"}), 404

    return jsonify(district_data[district]), 200

# ✅ Run the API Server
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
