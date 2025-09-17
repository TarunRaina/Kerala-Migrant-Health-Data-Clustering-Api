from utils_masking import mask_patient_data, unmask_patient_data
import pandas as pd

# Simulate DB fetch — list of dicts for multiple patients
db_data = [
    {'name': 'Tarun Raina', 'age': 30, 'gender': 'Male', 'address': 'Some Street'},
    {'name': 'Alice Smith', 'age': 25, 'gender': 'Female', 'address': 'Another Street'}
]

# Convert to DataFrame to match the function's expectation
df = pd.DataFrame(db_data)

# Masking
masked_df, mask_map = mask_patient_data(df)
print("Masked output:\n", masked_df)

# Unmasking
unmasked_df = unmask_patient_data(masked_df, mask_map)
print("Unmasked output:\n", unmasked_df)
