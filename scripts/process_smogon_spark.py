from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import sys
import json
import pandas as pd
from sqlalchemy import create_engine, text
import glob

def process_data():
    print("--- Starting Spark Job ---")
    
    db_url_spark = "jdbc:postgresql://postgres:5432/airflow?stringtype=unspecified"
    db_url_pandas = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
    
    try:
        engine = create_engine(db_url_pandas)
        with engine.connect() as conn:
            print("Cleaning up old data...")
            conn.execute(text("TRUNCATE TABLE dim_pokemon, fact_usage CASCADE;"))
            conn.commit()
        print("Tables cleaned successfully.")
    except Exception as e:
        print(f"Warning: Could not truncate tables (might be first run): {e}")

    spark = SparkSession.builder \
        .appName("SmogonProcessing") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

    raw_dir = "/opt/airflow/data_lake/raw"
    possible_files = glob.glob(f"{raw_dir}/*.json")
    smogon_file = None
    metadata_file = None

    print(f"Files found in {raw_dir}: {possible_files}")

    for f in possible_files:
        if "metadata" in f:
            metadata_file = f
        elif "smogon" in f or "gen9" in f:
            smogon_file = f
            
    if not smogon_file or not metadata_file:
        print("CRITICAL ERROR: Missing Smogon or Metadata file.")
        sys.exit(1)

    print(f"Using Smogon File: {smogon_file}")
    
    with open(metadata_file, 'r') as f:
        meta_dict = json.load(f)
    
    dim_rows = []
    valid_pokemon = set()
    
    for name, data in meta_dict.items():
        valid_pokemon.add(name)
        dim_rows.append({
            "pokemon_name": name,
            "type_1": data['types'][0] if len(data['types']) > 0 else None,
            "type_2": data['types'][1] if len(data['types']) > 1 else None,
            "hp": data['stats'].get('hp', 0),
            "attack": data['stats'].get('attack', 0),
            "defense": data['stats'].get('defense', 0),
            "sp_attack": data['stats'].get('special-attack', 0),
            "sp_defense": data['stats'].get('special-defense', 0),
            "speed": data['stats'].get('speed', 0),
            "is_fully_evolved": True
        })
    
    try:
        pdf_dim = pd.DataFrame(dim_rows)
        pdf_dim.to_sql('dim_pokemon', engine, if_exists='append', index=False)
        print(f"Dimension Table Loaded: {len(dim_rows)} rows.")
    except Exception as e:
        print(f"Error writing Dim: {e}")
        sys.exit(1)

    with open(smogon_file, 'r') as f:
        chaos_data = json.load(f)
    
    usage_data = chaos_data.get('data', chaos_data)

    fact_rows = []
    for name, stats in usage_data.items():
        if name in valid_pokemon: 
            items = stats.get('Items', {})
            moves = stats.get('Moves', {})
            spreads = stats.get('Spreads', {})
            
            top_items = dict(sorted(items.items(), key=lambda item: item[1], reverse=True)[:5])
            top_moves = dict(sorted(moves.items(), key=lambda item: item[1], reverse=True)[:10])
            top_spreads = dict(sorted(spreads.items(), key=lambda item: item[1], reverse=True)[:5])

            fact_rows.append((
                name,
                float(stats.get('usage', 0)),
                int(stats.get('Raw count', 0)),
                "2025-11-01", 
                json.dumps(top_items),
                json.dumps(top_moves),
                json.dumps(top_spreads)
            ))
            
    schema = StructType([
        StructField("pokemon_name", StringType(), True),
        StructField("usage_percent", DoubleType(), True),
        StructField("raw_count", IntegerType(), True),
        StructField("month_date", StringType(), True),
        StructField("top_items", StringType(), True),
        StructField("top_moves", StringType(), True),
        StructField("top_spreads", StringType(), True)
    ])
    
    if not fact_rows:
        print("WARNING: No rows matched! Check that Smogon names match API names.")
    else:
        sdf_fact = spark.createDataFrame(fact_rows, schema)
        
        sdf_fact.write \
            .format("jdbc") \
            .option("url", db_url_spark) \
            .option("dbtable", "fact_usage") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"Fact Table Loaded: {len(fact_rows)} rows.")
    
    spark.stop()

if __name__ == "__main__":
    process_data()