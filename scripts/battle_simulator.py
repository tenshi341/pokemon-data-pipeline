import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import random
import hashlib
import json
import time

DB_PARAMS = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

class BattleSimulator:
    def __init__(self):
        print("--- Initializing Simulator ---")
        self.conn = psycopg2.connect(**DB_PARAMS)
        self.cursor = self.conn.cursor()
        self.pokemon_pool = self._load_pokemon_pool()
        print(f"Loaded {len(self.pokemon_pool)} Pokemon into memory.")

    def _load_pokemon_pool(self):
        """
        Fetch Pokemon stats and usage to build the simulation pool.
        """
        query = """
        SELECT 
            p.pokemon_name, 
            p.type_1, 
            p.type_2, 
            (p.hp + p.attack + p.defense + p.sp_attack + p.sp_defense + p.speed) as bst,
            f.usage_percent
        FROM dim_pokemon p
        JOIN fact_usage f ON p.pokemon_name = f.pokemon_name
        WHERE f.usage_percent > 0.01 -- Filter out barely used mons for better teams
        """
        return pd.read_sql(query, self.conn)

    def generate_team(self):
        """
        Selects 6 random Pokemon based on usage weights.
        Returns: List of pokemon names, and a unique Team Hash.
        """
        team_df = self.pokemon_pool.sample(n=6, weights='usage_percent')
        team_names = sorted(team_df['pokemon_name'].tolist())
        
        team_str = ",".join(team_names)
        team_hash = hashlib.md5(team_str.encode()).hexdigest()
        
        return team_names, team_hash, team_df

    def simulate_match(self, team_a_df, team_b_df):
        """
        Simplified Battle Logic: Compare Total Base Stats (BST) + Type Randomness.
        In a real app, this would be a complex engine. For DE Portfolio, simple is fine.
        """
        power_a = team_a_df['bst'].sum() * random.uniform(0.8, 1.2)
        power_b = team_b_df['bst'].sum() * random.uniform(0.8, 1.2)
        
        turns = random.randint(5, 20)
        
        if power_a > power_b:
            return "A", turns
        else:
            return "B", turns

    def save_results_batch(self, battle_logs, new_teams):
        """
        Writes a batch of battles to Postgres.
        Crucial: Handles 'Lazy Loading' of teams.
        """
        if new_teams:
            insert_team_query = """
            INSERT INTO fact_battles (team_hash, winner_pokemon_id, turns) 
            VALUES %s 
            -- We are actually misusing the table slightly here for the portfolio simplicity.
            -- Ideally we'd have a 'dim_teams' table. 
            -- For this demo, we just assume the team hash is enough tracking.
            """
            pass

        insert_query = """
        INSERT INTO fact_battles (team_hash, winner_pokemon_id, turns)
        VALUES %s
        """
        execute_values(self.cursor, insert_query, battle_logs)
        self.conn.commit()
        print(f"Batch saved: {len(battle_logs)} battles.")

    def run_simulation(self, num_battles=100):
        print(f"Simulating {num_battles} battles...")
        
        battle_queue = []
        batch_size = 50
        
        for i in range(num_battles):
            names_a, hash_a, df_a = self.generate_team()
            names_b, hash_b, df_b = self.generate_team()
            
            winner, turns = self.simulate_match(df_a, df_b)
            
            winning_hash = hash_a if winner == "A" else hash_b
            mvp = names_a[0] if winner == "A" else names_b[0]
            
            battle_queue.append((winning_hash, mvp, turns))
            
            if len(battle_queue) >= batch_size:
                self.save_results_batch(battle_queue, [])
                battle_queue = []
        
        if battle_queue:
            self.save_results_batch(battle_queue, [])

if __name__ == "__main__":
    sim = BattleSimulator()
    sim.run_simulation(num_battles=100)