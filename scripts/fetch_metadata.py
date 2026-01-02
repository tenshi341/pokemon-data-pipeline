import requests
import json
import os
import time

def fetch_pokemon_metadata():
    raw_dir = "/opt/airflow/data_lake/raw"
    files = [f for f in os.listdir(raw_dir) if f.startswith("smogon_") and f.endswith(".json")]
    if not files:
        print("No Smogon data found to enrich!")
        return
    
    smogon_file_path = os.path.join(raw_dir, files[0])
    output_file = os.path.join(raw_dir, "pokemon_metadata.json")
    
    print(f"Reading Pokemon list from: {smogon_file_path}")

    with open(smogon_file_path, 'r') as f:
        smogon_data = json.load(f)
        pokemon_names = list(smogon_data['data'].keys())

    print(f"Found {len(pokemon_names)} Pokemon to fetch metadata for.")

    metadata = {}
    
    session = requests.Session()
    
    for i, name in enumerate(pokemon_names):
        api_name = name.lower().replace(" ", "-").replace(".", "").replace(":", "")
        
        url = f"https://pokeapi.co/api/v2/pokemon/{api_name}"
        
        try:
            response = session.get(url)
            if response.status_code == 200:
                data = response.json()
                
                metadata[name] = {
                    "id": data['id'],
                    "types": [t['type']['name'] for t in data['types']],
                    "stats": {s['stat']['name']: s['base_stat'] for s in data['stats']},
                    "height": data['height'],
                    "weight": data['weight']
                }
                print(f"[{i+1}/{len(pokemon_names)}] Fetched: {name}")
            else:
                print(f"[{i+1}/{len(pokemon_names)}] Failed (404): {name}")
                
        except Exception as e:
            print(f"Error fetching {name}: {e}")
            
        time.sleep(0.1)

    with open(output_file, 'w') as f:
        json.dump(metadata, f)
    
    print(f"Metadata saved to {output_file}")

if __name__ == "__main__":
    fetch_pokemon_metadata()