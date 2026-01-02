-- 1. Dimension: Pokemon Metadata
CREATE TABLE IF NOT EXISTS dim_pokemon (
    pokemon_name VARCHAR(50) PRIMARY KEY,
    type_1 VARCHAR(20),
    type_2 VARCHAR(20),
    hp INT,
    attack INT,
    defense INT,
    sp_attack INT,
    sp_defense INT,
    speed INT,
    is_fully_evolved BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS fact_usage (
    usage_id SERIAL PRIMARY KEY,
    pokemon_name VARCHAR(50) REFERENCES dim_pokemon(pokemon_name),
    usage_percent FLOAT,
    raw_count INT,
    month_date DATE,
    top_items JSONB,
    top_moves JSONB,
    top_spreads JSONB
);

CREATE TABLE IF NOT EXISTS fact_battles (
    battle_id SERIAL PRIMARY KEY,
    team_hash VARCHAR(64),
    winner_pokemon_id VARCHAR(50),
    turns INT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);