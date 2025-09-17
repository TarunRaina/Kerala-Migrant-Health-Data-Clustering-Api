# hosted_api/app.py
from flask import Flask, send_from_directory, request, jsonify
import requests
import os

app = Flask(__name__, static_folder='../frontend', static_url_path='/')

# Serve frontend files
@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    # Serve JS, CSS, GeoJSON, etc.
    return send_from_directory(app.static_folder, path)

# Proxy district_info API calls to internal API
@app.route('/district_info')
def proxy_district_info():
    internal_api_url = 'http://127.0.0.1:5000/district_info'
    district = request.args.get('district')

    if not district:
        return jsonify({'error': 'No district provided'}), 400

    try:
        # Forward request to internal API
        resp = requests.get(internal_api_url, params={'district': district})
        return jsonify(resp.json())
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Failed to reach internal API: {e}'}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))  # default 8000
    app.run(host='0.0.0.0', port=port, debug=True)
