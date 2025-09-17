# ml/train_model.py
import os
import sys
import json
import pandas as pd
import numpy as np
import pickle
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from utils_masking import mask_patient_data, unmask_patient_data

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_CONNECTION_STRING")
DB_NAME = "kerala_health_system"

# MongoDB connection
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
patients_col = db.patients
disease_col = db.disease_cases
districts_col = db.districts

# Paths
CSV_FILE = "../data/kerala_master_dataset.csv"
KMEANS_MODEL_PATH = "../models/kmeans_risk_model.pkl"
SCALER_PATH = "../models/scaler.pkl"
CLUSTERED_CSV = "../data/kerala_clustered_districts.csv"
DISTRICT_JSON_PATH = "../district_data/district_data.json"

# --- Clustering Function ---
def load_and_cluster(csv_file=CSV_FILE, n_clusters=4):
    df = pd.read_csv(csv_file)
    features = ['piped_water_dwelling_pct', 'own_well_pct', 'community_water_pct',
                'surface_water_pct', 'one_toilet_pct', 'two_toilet_pct', 'three_plus_toilet_pct',
                'water_risk_rating', 'sanitation_risk_rating', 'crowding_risk_rating',
                'healthcare_access_risk_rating', 'overall_risk_rating']

    df_features = df[features].fillna(0)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_features)

    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    clusters = kmeans.fit_predict(X_scaled)
    df['risk_cluster'] = clusters

    # Save models
    os.makedirs("../models", exist_ok=True)
    with open(KMEANS_MODEL_PATH, "wb") as f:
        pickle.dump(kmeans, f)
    with open(SCALER_PATH, "wb") as f:
        pickle.dump(scaler, f)

    df.to_csv(CLUSTERED_CSV, index=False)
    print(f"✅ Clustering done. Models saved in '../models/'\n", df[['district', 'risk_cluster']])
    return df, scaler, kmeans

# --- Fetch and Mask ---
def fetch_live_data(mask=True):
    patient_docs = list(patients_col.find({}))
    case_docs = list(disease_col.find({}))

    if not patient_docs or not case_docs:
        print("⚠️ No live data found in MongoDB. Make sure atlas_setup ran.")
        return pd.DataFrame(), pd.DataFrame(), {}

    df_patients = pd.DataFrame(patient_docs)
    df_cases = pd.DataFrame(case_docs)

    # Convert age from MongoDB extended JSON if needed
    if 'age' in df_patients.columns and isinstance(df_patients.loc[0, 'age'], dict):
        df_patients['age'] = df_patients['age'].apply(lambda x: int(x.get('$numberInt', 0)))

    mask_map = {}
    if mask:
        df_patients, mask_map = mask_patient_data(df_patients)

    return df_patients, df_cases, mask_map

# --- Risk Tag ---
def get_risk_tag(district_info):
    tags = []
    if district_info['water_risk_rating'] > 5 or district_info['sanitation_risk_rating'] > 5:
        tags.append('water_sanitation')
    if district_info['crowding_risk_rating'] > 5:
        tags.append('crowding')
    if district_info['healthcare_access_risk_rating'] > 5:
        tags.append('healthcare_access')
    if not tags:
        tags.append('general')
    return ', '.join(tags)

# --- Disease Analysis ---
def analyze_disease_patterns(df_patients, df_cases, clustered_df, mask_map=None):
    summary = {}
    if df_patients.empty or df_cases.empty:
        return summary

    if mask_map:
        df_patients = unmask_patient_data(df_patients, mask_map)

    RISK_THRESHOLDS = {
        'water_risk_rating': 5,
        'sanitation_risk_rating': 5,
        'crowding_risk_rating': 5,
        'healthcare_access_risk_rating': 5
    }

    districts = clustered_df['district'].unique()
    for district in districts:
        district_patients = df_patients[df_patients['district'] == district]
        district_cases = df_cases[df_cases['district'] == district]
        district_info = clustered_df[clustered_df['district'] == district].iloc[0]

        disease_summary = {}
        diseases = district_cases['disease_name'].unique() if not district_cases.empty else []

        for disease in diseases:
            disease_cases = district_cases[district_cases['disease_name'] == disease]
            affected_patients = district_patients[district_patients['patient_id'].isin(disease_cases['patient_id'])]

            age_counts = {
                '0-14': len(affected_patients[affected_patients['age'] <= 14]),
                '15-24': len(affected_patients[(affected_patients['age'] >= 15) & (affected_patients['age'] <= 24)]),
                '25-44': len(affected_patients[(affected_patients['age'] >= 25) & (affected_patients['age'] <= 44)]),
                '45-64': len(affected_patients[(affected_patients['age'] >= 45) & (affected_patients['age'] <= 64)]),
                '65+': len(affected_patients[affected_patients['age'] >= 65])
            }
            max_age_group = max(age_counts, key=age_counts.get)

            gender_counts = affected_patients['gender'].value_counts().to_dict()
            main_gender = max(gender_counts, key=gender_counts.get) if gender_counts else None

            possible_causes = []
            if district_info['water_risk_rating'] > RISK_THRESHOLDS['water_risk_rating']:
                possible_causes.append("High water risk")
            if district_info['sanitation_risk_rating'] > RISK_THRESHOLDS['sanitation_risk_rating']:
                possible_causes.append("Poor sanitation")
            if district_info['crowding_risk_rating'] > RISK_THRESHOLDS['crowding_risk_rating']:
                possible_causes.append("High population density / crowding")
            if district_info['healthcare_access_risk_rating'] > RISK_THRESHOLDS['healthcare_access_risk_rating']:
                possible_causes.append("Low healthcare access")

            disease_summary[disease] = {
                'cases': len(disease_cases),
                'mainly_affected': {'age_group': max_age_group, 'gender': main_gender},
                'possible_causes': possible_causes,
                'district_info': district_info
            }

        summary[district] = {'disease_summary': disease_summary}

    return summary

# --- Print ---
def print_summary(summary, top_n=None):
    for district, info in summary.items():
        print(f"\n🏥 {district} Disease Summary:")
        disease_data = info['disease_summary']

        sorted_diseases = sorted(disease_data.items(), key=lambda x: x[1]['cases'], reverse=True)
        if top_n:
            sorted_diseases = sorted_diseases[:top_n]

        for disease, data in sorted_diseases:
            cases = data['cases']
            main = data['mainly_affected']
            causes = data['possible_causes']
            risk_tag = get_risk_tag(data['district_info'])

            print(f"  🔹 {disease} [{risk_tag}]: {cases} cases")
            print(f"      - Primarily affected: Age {main.get('age_group','N/A')}, "
                  f"Sex: {main.get('gender','N/A')}")
            print(f"      - Possible causes: {', '.join(causes) if causes else 'Unknown'}")

# --- NEW: Fetchable dict for full district info ---
def get_fresh_district_data(df_patients, df_cases, clustered_df, mask_map=None):
    summary = analyze_disease_patterns(df_patients, df_cases, clustered_df, mask_map)
    district_data = {}
    for district, info in summary.items():
        district_data[district] = info
    return district_data

# --- NEW: Regenerate JSON for API ---
def regenerate_district_json():
    os.makedirs(os.path.dirname(DISTRICT_JSON_PATH), exist_ok=True)
    clustered_df, scaler, kmeans = load_and_cluster()
    df_patients, df_cases, mask_map = fetch_live_data(mask=True)
    summary = analyze_disease_patterns(df_patients, df_cases, clustered_df, mask_map)
    print_summary(summary)
    district_data = get_fresh_district_data(df_patients, df_cases, clustered_df, mask_map)
    with open(DISTRICT_JSON_PATH, "w") as f:
        json.dump(district_data, f, default=str, indent=4)
    print(f"\n✅ District JSON refreshed at '{DISTRICT_JSON_PATH}'")

# --- Main ---
if __name__ == "__main__":
    regenerate_district_json()
