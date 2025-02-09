import requests
import pandas as pd
import os

def extract_data():
    url = "https://restcountries.com/v3.1/all"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching data:", response.status_code)
        return []

# --- Step 2: Transform data (cleaning + structuring) ---
def transform_data(data):
    extracted_data = []

    for country in data:
        extracted_data.append({
            "Country": country.get("name", {}).get("common", "Unknown"),
            "Official Name": country.get("name", {}).get("official", "Unknown"),
            "Region": country.get("region", "Unknown"),
            "Subregion": country.get("subregion", "Unknown"),
            "Capital": ", ".join(country.get("capital", ["Unknown"])),
            "Population": country.get("population", 0),
            "Latitude": country.get("latlng", [None, None])[0],
            "Longitude": country.get("latlng", [None, None])[1],
            "Area (sq km)": country.get("area", 0),
            "Timezones": ", ".join(country.get("timezones", ["Unknown"])),
            "Flag": country.get("flags", {}).get("png", "No flag available")
        })

    # Convert to DataFrame
    df = pd.DataFrame(extracted_data)

    # Data Cleaning: Remove missing values (if any)
    df.fillna("Unknown", inplace=True)

    return df

# --- Step 3: Load data ---
def load_data(df, filename="countries.csv"):
    os.makedirs("data", exist_ok=True)  # Create 'data' folder if it doesn't exist
    df.to_csv(f"data/{filename}", index=False)  # Save the DataFrame as a CSV
    print(f"Data saved to data/{filename}")

if __name__ == "__main__":
    print("Testing the full ETL process...")

    # Step 1: Extract
    print("Extracting data...")
    data = extract_data()

    if data:  # If data is not empty
        # Step 2: Transform
        print("Transforming data...")
        df = transform_data(data)

        # Step 3: Load
        print("Loading data...")
        load_data(df)

        print("ETL process completed successfully!")
    else:
        print("No data to process.")