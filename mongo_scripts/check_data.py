# mongo_scripts/dump_all_data.py
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

def main():
    connection_string = os.getenv('MONGODB_CONNECTION_STRING')
    if not connection_string:
        raise ValueError("Set MONGODB_CONNECTION_STRING in .env")

    client = MongoClient(connection_string)
    db = client.kerala_health_system

    # Dump patients
    with open("patients_dump.txt", "w", encoding="utf-8") as f:
        for patient in db.patients.find():
            f.write(str(patient) + "\n")
    print("✅ All patient data written to patients_dump.txt")

    # Dump disease cases
    with open("disease_cases_dump.txt", "w", encoding="utf-8") as f:
        for case in db.disease_cases.find():
            f.write(str(case) + "\n")
    print("✅ All disease case data written to disease_cases_dump.txt")

if __name__ == "__main__":
    main()
