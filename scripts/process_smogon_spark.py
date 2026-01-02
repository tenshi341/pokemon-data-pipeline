from pyspark.sql import SparkSession #type:ignore
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType #type:ignore
import os
import sys
import json
import pandas as pd #type:ignore
from sqlalchemy import create_engine, text #type:ignore
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
        print(f"Warning: Could not truncate tables: {e}")

    spark = SparkSession.builder \
        .appName("SmogonProcessing") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

    raw_dir = "/opt/airflow/data_lake/raw"
    possible_files = glob.glob(f"{raw_dir}/*.json")
    smogon_file = None
    metadata_file = None

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
            "pokemon_id": data['id'],
            "pokemon_name": name,
            "type_1": data['types'][0] if len(data['types']) > 0 else None,
            "type_2": data['types'][1] if len(data['types']) > 1 else None,
            "bst": sum(data['stats'].values()),
            "is_fully_evolved": True
        })
    
    try:
        pdf_dim = pd.DataFrame(dim_rows)
        pdf_dim.to_sql('dim_pokemon', engine, if_exists='append', index=False)
        print(f"Dimension Table Loaded: {len(dim_rows)} rows.")
    except Exception as e:
        print(f"Error writing Dim: {e}")
        pass

    with open(smogon_file, 'r') as f:
        chaos_data = json.load(f)
    
    usage_data = chaos_data.get('data', chaos_data)

    fact_rows = []
    for name, stats in usage_data.items():
        if name in valid_pokemon: 
            items = stats.get('Items', {})
            moves = stats.get('Moves', {})
            spreads = stats.get('Spreads', {})
            
            natures_count = {}
            for k, v in spreads.items():
                nature_name = k.split(':')[0]
                natures_count[nature_name] = natures_count.get(nature_name, 0) + v
            
            top_items = dict(sorted(items.items(), key=lambda item: item[1], reverse=True)[:5])
            top_moves = dict(sorted(moves.items(), key=lambda item: item[1], reverse=True)[:10])
            top_spreads = dict(sorted(spreads.items(), key=lambda item: item[1], reverse=True)[:5])
            top_natures = dict(sorted(natures_count.items(), key=lambda item: item[1], reverse=True)[:5])

            p_id = next((d['pokemon_id'] for d in dim_rows if d['pokemon_name'] == name), None)

            if p_id:
                fact_rows.append((
                    p_id,
                    float(stats.get('usage', 0)),
                    int(stats.get('Raw count', 0)),
                    "2025-11-01", 
                    json.dumps(top_items),
                    json.dumps(top_moves),
                    json.dumps(top_spreads),
                    json.dumps(top_natures)
                ))
            
    schema = StructType([
        StructField("pokemon_id", IntegerType(), True),
        StructField("usage_percent", DoubleType(), True),
        StructField("raw_count", IntegerType(), True),
        StructField("month_date", StringType(), True),
        StructField("top_items", StringType(), True),
        StructField("top_moves", StringType(), True),
        StructField("top_spreads", StringType(), True),
        StructField("top_natures", StringType(), True)
    ])
    
    if fact_rows:
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