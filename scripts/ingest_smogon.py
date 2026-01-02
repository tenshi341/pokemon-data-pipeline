import requests
import os
import sys
import json
import pandas as pd #type:ignore
from sqlalchemy import create_engine, text #type:ignore

DB_CONNECTION = "postgresql://airflow:airflow@postgres/airflow"

def get_top_key(data_dict):
    """Safely gets the key with the highest value from a dictionary."""
    if not data_dict:
        return None
    return sorted(data_dict.items(), key=lambda x: x[1], reverse=True)[0][0]

def get_top_n_keys(data_dict, n=4):
    """Safely gets the top N keys (for moves)."""
    if not data_dict:
        return [None] * n
    sorted_items = sorted(data_dict.items(), key=lambda x: x[1], reverse=True)
    keys = [item[0] for item in sorted_items[:n]]
    while len(keys) < n:
        keys.append(None)
    return keys

def ingest_smogon_data(year_month, tier="gen9ou", rating="1695"):
    """
    1. Downloads Chaos JSON from Smogon to Data Lake.
    2. Parses the JSON for Usage, Items, Moves, Natures.
    3. Loads the clean data into Postgres.
    """
    url = f"https://www.smogon.com/stats/{year_month}/chaos/{tier}-{rating}.json"
    
    output_dir = "/opt/airflow/data_lake/raw"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"smogon_{tier}_{year_month}.json")

    print(f"--- 1. Extraction (Download) ---")
    print(f"Target URL: {url}")
    
    if os.path.exists(output_file):
        print(f"File {output_file} already exists. Using cached version.")
    else:
        try:
            print("Downloading...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(output_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("Download complete.")
        except Exception as e:
            print(f"CRITICAL ERROR downloading data: {e}")
            sys.exit(1)

    print(f"--- 2. Transformation (Parsing) ---")
    try:
        with open(output_file, 'r') as f:
            raw_data = json.load(f)
    except json.JSONDecodeError:
        print("Error: Downloaded file is not valid JSON.")
        sys.exit(1)

    pokemon_map = raw_data.get('data', {})
    print(f"--> Found stats for {len(pokemon_map)} Pokemon.")

    processed_rows = []

    for name, stats in pokemon_map.items():
        usage_count = stats.get('usage', 0)

        moves = stats.get('Moves', {})
        items = stats.get('Items', {})
        spreads = stats.get('Spreads', {})

        most_used_item = get_top_key(items)

        top_spread = get_top_key(spreads)
        most_common_nature = top_spread.split(':')[0] if top_spread else "Serious"

        top_moves = get_top_n_keys(moves, n=4)

        row = {
            "pokemon_name": name,
            "usage_percent": usage_count,
            "most_common_nature": most_common_nature,
            "most_used_item": most_used_item,
            "move_1": top_moves[0],
            "move_2": top_moves[1],
            "move_3": top_moves[2],
            "move_4": top_moves[3]
        }
        processed_rows.append(row)

    df = pd.DataFrame(processed_rows)
    
    df = df.sort_values(by='usage_percent', ascending=False).head(100)
    print(f"--> Transformed Top {len(df)} Meta Pokemon.")

    print(f"--- 3. Loading to Database ---")
    engine = create_engine(DB_CONNECTION)

    df.to_sql('fact_pokemon_usage', engine, if_exists='replace', index=False)
    
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_battles (
                battle_id VARCHAR(50) PRIMARY KEY,
                format VARCHAR(20),
                winner_player VARCHAR(50),
                turns INT,
                replay_log TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
    
    print(f"--- SUCCESS: Pipeline Finished. Data available for Bot. ---")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = "2025-11"
        
    ingest_smogon_data(target_date)