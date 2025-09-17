import os
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker

load_dotenv()
fake = Faker('en_IN')

class KeralaHealthAtlasSetup:
    def __init__(self):
        self.connection_string = os.getenv('MONGODB_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("Set MONGODB_CONNECTION_STRING in .env")
        
        self.client = MongoClient(self.connection_string)
        self.db = self.client.kerala_health_system

        # Collections
        self.districts = self.db.districts
        self.hospitals = self.db.hospitals
        self.patients = self.db.patients
        self.disease_cases = self.db.disease_cases

        # Test connection
        self.client.admin.command('ping')
        print("✅ Connected to MongoDB Atlas")

    def load_district_data(self, csv_file='data/kerala_master_dataset.csv'):
        df = pd.read_csv(csv_file)
        print(f"✅ Loaded {len(df)} districts from CSV")
        self.districts.delete_many({})

        docs = []
        for _, row in df.iterrows():
            docs.append({
                'district_name': row['district'],
                'region': row.get('region', ''),
                'coordinates': {'lat': float(row['latitude']), 'lon': float(row['longitude'])},
                'demographics': {
                    'population_2023': int(row['population_2023']),
                    'total_emigrants_2023': int(row['total_emigrants_2023']),
                    'migrant_density_per_1000': float(row['migrant_density_per_1000'])
                },
                'infrastructure': {
                    'piped_water_pct': float(row['piped_water_dwelling_pct']),
                    'own_well_pct': float(row['own_well_pct']),
                    'community_water_pct': float(row['community_water_pct'])
                },
                'risk_ratings': {
                    'water_risk': float(row['water_risk_rating']),
                    'sanitation_risk': float(row['sanitation_risk_rating']),
                    'crowding_risk': float(row['crowding_risk_rating']),
                    'overall_risk': float(row['overall_risk_rating'])
                },
                'created_at': datetime.now()
            })

        self.districts.insert_many(docs)
        print(f"✅ Inserted {len(docs)} districts into Atlas")

    def generate_hospitals(self):
        self.hospitals.delete_many({})
        hospital_types = ['Government Hospital', 'Primary Health Center', 'District Hospital', 'Medical College', 'Private Hospital', 'Community Health Center']
        hospitals = []

        for district_doc in self.districts.find():
            pop = district_doc['demographics']['population_2023']
            num_hospitals = max(2, int(pop / 300000))
            for i in range(num_hospitals):
                hospitals.append({
                    'hospital_id': f"{district_doc['district_name'][:3].upper()}_H{i+1:02d}",
                    'name': f"{fake.company()} {random.choice(hospital_types)}",
                    'district': district_doc['district_name'],
                    'type': random.choice(hospital_types),
                    'bed_capacity': random.randint(20, 300),
                    'coordinates': {
                        'lat': district_doc['coordinates']['lat'] + random.uniform(-0.05, 0.05),
                        'lon': district_doc['coordinates']['lon'] + random.uniform(-0.05, 0.05)
                    },
                    'monthly_capacity': random.randint(200, 2000),
                    'created_at': datetime.now()
                })
        self.hospitals.insert_many(hospitals)
        print(f"✅ Generated {len(hospitals)} hospitals")

    def generate_disease_data(self, months=6):
        self.patients.delete_many({})
        self.disease_cases.delete_many({})
        diseases = {
            'water_borne': ['Cholera', 'Typhoid', 'Hepatitis A', 'Diarrhea'],
            'vector_borne': ['Dengue', 'Chikungunya', 'Malaria'],
            'respiratory': ['Tuberculosis', 'Pneumonia', 'Bronchitis']
        }

        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=30*months)
        patient_id, case_id = 1, 1
        patients_batch, cases_batch = [], []

        current_date = start_date
        total_generated = 0

        while current_date <= end_date:
            for district_doc in self.districts.find():
                district = district_doc['district_name']
                hospitals = list(self.hospitals.find({'district': district}))
                if not hospitals:
                    continue

                pop_factor = district_doc['demographics']['population_2023'] / 1000000
                risk_factor = district_doc['risk_ratings']['overall_risk'] / 10
                daily_cases = max(5, int(random.uniform(20, 50) * pop_factor * (0.5 + risk_factor)))

                for _ in range(daily_cases):
                    hospital = random.choice(hospitals)
                    migrant_prob = min(0.4, district_doc['demographics']['migrant_density_per_1000'] / 200)
                    is_migrant = random.random() < migrant_prob

                    patient = {
                        'patient_id': f"KL{patient_id:08d}",
                        'name': fake.name(),
                        'age': max(1, int(np.random.normal(35, 20))),
                        'gender': random.choice(['Male', 'Female']),
                        'district': district,
                        'is_migrant': is_migrant,
                        'created_at': current_date
                    }
                    patients_batch.append(patient)

                    # Disease selection
                    water_risk = district_doc['risk_ratings']['water_risk']
                    crowding_risk = district_doc['risk_ratings']['crowding_risk']
                    if random.random() < (water_risk / 20):
                        disease = random.choice(diseases['water_borne'])
                        category = 'water_borne'
                    elif random.random() < (crowding_risk / 20):
                        disease = random.choice(diseases['vector_borne'])
                        category = 'vector_borne'
                    else:
                        disease = random.choice(diseases['respiratory'])
                        category = 'respiratory'

                    case = {
                        'case_id': f"CASE{case_id:08d}",
                        'patient_id': patient['patient_id'],
                        'hospital_id': hospital['hospital_id'],
                        'district': district,
                        'disease_name': disease,
                        'disease_category': category,
                        'admission_date': current_date,
                        'is_migrant_patient': is_migrant,
                        'severity': random.choice(['Mild', 'Moderate', 'Severe']),
                        'outcome': 'Recovered',
                        'district_risk_at_admission': {
                            'water_risk': water_risk,
                            'crowding_risk': crowding_risk,
                            'overall_risk': district_doc['risk_ratings']['overall_risk']
                        },
                        'created_at': current_date
                    }
                    cases_batch.append(case)

                    patient_id += 1
                    case_id += 1
                    total_generated += 1

                    if len(patients_batch) >= 5000:  # insert in big batches for performance
                        self.patients.insert_many(patients_batch)
                        self.disease_cases.insert_many(cases_batch)
                        patients_batch, cases_batch = [], []

                        print(f"🩺 Inserted {total_generated} patients and cases so far...")

            current_date += timedelta(days=1)
            if (current_date - start_date).days % 30 == 0:
                print(f"📅 Data generated up to {current_date.strftime('%Y-%m-%d')}")

        # Insert remaining
        if patients_batch:
            self.patients.insert_many(patients_batch)
            self.disease_cases.insert_many(cases_batch)

        print(f"✅ Total generated: {self.patients.count_documents({})} patients and {self.disease_cases.count_documents({})} disease cases")

    def validate_correlations(self):
        print("\n🔍 Validating correlations...")
        pipeline = [
            {'$lookup': {'from': 'districts', 'localField': 'district', 'foreignField': 'district_name', 'as': 'district_info'}},
            {'$unwind': '$district_info'},
            {'$match': {'disease_category': 'water_borne'}},
            {'$group': {'_id': '$district', 'cases': {'$sum': 1}, 'avg_water_risk': {'$avg': '$district_info.risk_ratings.water_risk'}}},
            {'$sort': {'cases': -1}}
        ]
        results = list(self.disease_cases.aggregate(pipeline))
        print("Top 5 districts for water-borne diseases:")
        for r in results[:5]:
            print(f"  {r['_id']}: {r['cases']} cases (Water Risk: {r['avg_water_risk']:.1f})")

def run_full_setup():
    atlas = KeralaHealthAtlasSetup()
    atlas.load_district_data()
    atlas.generate_hospitals()
    atlas.generate_disease_data(months=6)
    atlas.validate_correlations()
    print("🎉 MongoDB Atlas setup complete!")

if __name__ == "__main__":
    run_full_setup()
