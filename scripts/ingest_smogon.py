import requests
import os
import sys

def ingest_smogon_data(year_month, tier="gen9ou", rating="1825"):
    """
    Downloads Chaos JSON data from Smogon for a specific month/tier.
    """
    url = f"https://www.smogon.com/stats/{year_month}/chaos/{tier}-{rating}.json"
    
    output_dir = "/opt/airflow/data_lake/raw"
    output_file = os.path.join(output_dir, f"smogon_{tier}_{year_month}.json")

    print(f"--- Starting Ingestion ---")
    print(f"Target URL: {url}")
    print(f"Target File: {output_file}")

    if os.path.exists(output_file):
        print("File already exists. Skipping download.")
        return

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print("Download complete.")

    except requests.exceptions.HTTPError as e:
        print(f"Error downloading data: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = "2025-11"
        
    ingest_smogon_data(target_date)