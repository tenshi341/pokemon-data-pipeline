CREATE TABLE IF NOT EXISTS ref_nature (
    nature_id SERIAL PRIMARY KEY,
    nature_name VARCHAR(20) UNIQUE
);

CREATE TABLE IF NOT EXISTS ref_item (
    item_id SERIAL PRIMARY KEY,
    item_name VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS ref_move (
    move_id SERIAL PRIMARY KEY,
    move_name VARCHAR(50) UNIQUE,
    move_type VARCHAR(20),
    power INT,
    accuracy INT
);

CREATE TABLE IF NOT EXISTS dim_pokemon (
    pokemon_id INT PRIMARY KEY,
    pokemon_name VARCHAR(50) UNIQUE,
    type_1 VARCHAR(20),
    type_2 VARCHAR(20),
    bst INT,
    is_fully_evolved BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS fact_usage (
    usage_id SERIAL PRIMARY KEY,
    pokemon_id INT REFERENCES dim_pokemon(pokemon_id),
    usage_percent FLOAT,
    raw_count INT,
    month_date DATE,
    top_items JSONB, 
    top_moves JSONB,
    top_spreads JSONB,
    top_natures JSONB
);

CREATE TABLE IF NOT EXISTS fact_battles (
    battle_id VARCHAR(50) PRIMARY KEY,
    format VARCHAR(20),
    winner_pokemon_name VARCHAR(50),
    winner_player VARCHAR(20),
    turns INT,
    replay_log TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO ref_nature (nature_name) VALUES 
('Adamant'), ('Bashful'), ('Bold'), ('Brave'), ('Calm'), ('Careful'), ('Docile'), 
('Gentle'), ('Hardy'), ('Hasty'), ('Impish'), ('Jolly'), ('Lax'), ('Lonely'), 
('Mild'), ('Modest'), ('Naive'), ('Naughty'), ('Quiet'), ('Quirky'), ('Rash'), 
('Relaxed'), ('Sassy'), ('Serious'), ('Timid')
ON CONFLICT (nature_name) DO NOTHING;