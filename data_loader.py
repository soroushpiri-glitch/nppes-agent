import pandas as pd


def load_data():
    df = pd.read_csv("nppes_api_1050_records.csv")

    text_cols = [
        "Entity_Type", "First_Name", "Middle_Name", "Last_Name", "Credential",
        "Gender", "Status", "Specialty", "Taxonomy_Code", "City", "State",
        "Postal_Code", "Phone"
    ]

    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df
