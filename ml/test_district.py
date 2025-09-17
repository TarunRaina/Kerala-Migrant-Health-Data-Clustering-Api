# test_district.py
from train_model import fetch_live_data, analyze_district, analyze_disease_patterns, print_summary, load_and_cluster

if __name__ == "__main__":
    # --- Load clustering info ---
    clustered_df, scaler, kmeans = load_and_cluster()

    # --- Fetch live data from MongoDB (masked) ---
    df_patients, df_cases, mask_map = fetch_live_data(mask=True)

    if df_patients.empty or df_cases.empty:
        print("⚠️ No live data available for testing.")
    else:
        # --- Analyze all districts ---
        summary = analyze_disease_patterns(df_patients, df_cases, clustered_df, mask_map)
        print_summary(summary)

        # --- Optional: Analyze a single district ---
        district_name = "Kottayam"
        single_district_summary = analyze_district(district_name, df_patients, df_cases, clustered_df, mask_map)
        print(f"\n📍 Single District ({district_name}) Summary:\n", single_district_summary)
