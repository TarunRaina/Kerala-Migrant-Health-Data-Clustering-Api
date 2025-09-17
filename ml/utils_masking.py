# ml/utils_masking.py
import hashlib
import random
import string

# Fields we want to mask
MASK_FIELDS = ['patient_id', 'name', 'age', 'address']

def mask_patient_data(df):
    """
    Mask sensitive fields in patient DataFrame.
    Returns masked DataFrame and a mapping for unmasking.
    """
    df = df.copy()
    mask_map = {}

    for field in MASK_FIELDS:
        if field in df.columns:
            masked_values = []
            for val in df[field]:
                if field == 'address':
                    # replace with a random string of similar length
                    masked_val = ''.join(random.choices(string.ascii_letters + string.digits, k=max(5, len(str(val)))))
                else:
                    # hash name, patient_id, age
                    masked_val = hashlib.sha256(str(val).encode()).hexdigest()[:10]
                masked_values.append(masked_val)
                mask_map[masked_val] = val
            df[field] = masked_values

    return df, mask_map

def unmask_patient_data(df, mask_map):
    """
    Unmask previously masked patient DataFrame using the map.
    """
    df = df.copy()
    for field in MASK_FIELDS:
        if field in df.columns:
            df[field] = df[field].apply(lambda x: mask_map.get(x, x))
    return df
