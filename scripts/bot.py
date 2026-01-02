import asyncio
import psycopg2
import logging
import sys
import os
import time
import random

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from poke_env import ServerConfiguration, AccountConfiguration
from poke_env.player import RandomPlayer
from teambuilder import TeamBuilder 

logging.basicConfig(level=logging.ERROR)
logging.getLogger("poke_env").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.ERROR)

DB_PARAMS = {"host": "postgres", "database": "airflow", "user": "airflow", "password": "airflow"}
SHOWDOWN_CONFIG = ServerConfiguration("ws://showdown:8000/showdown/websocket", None)

class DataHandler:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_PARAMS)
        self.cursor = self.conn.cursor()

    def register_team(self, team_hash, team_text):
        """Registers the team text if it's new."""
        query = "INSERT INTO dim_teams (team_hash, team_text) VALUES (%s, %s) ON CONFLICT (team_hash) DO NOTHING"
        self.cursor.execute(query, (team_hash, team_text))
        self.conn.commit()

    def update_stats(self, team_hash, matches_played, wins_gained):
        """Upsert logic for team stats."""
        query = """
        INSERT INTO fact_team_stats (team_hash, total_matches, total_wins, last_updated)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (team_hash) 
        DO UPDATE SET 
            total_matches = fact_team_stats.total_matches + EXCLUDED.total_matches,
            total_wins = fact_team_stats.total_wins + EXCLUDED.total_wins,
            last_updated = NOW();
        """
        self.cursor.execute(query, (team_hash, matches_played, wins_gained))
        self.conn.commit()

class Gen9Bot(RandomPlayer):
    def teampreview(self, battle):
        return "/team 123456"

async def run_single_set(iteration):
    n_battles = random.randint(64, 256)
    
    print(f"\n=== Set #{iteration}: Simulating {n_battles} Matches ===")
    
    builder = TeamBuilder()
    db = DataHandler()

    try:
        t1_text, t1_hash = builder.generate_showdown_team()
        t2_text, t2_hash = builder.generate_showdown_team()
    except Exception as e:
        print(f"CRITICAL: Team generation failed ({e}). Skipping set.")
        return

    db.register_team(t1_hash, t1_text)
    db.register_team(t2_hash, t2_text)
    
    p1 = Gen9Bot(
        battle_format="gen9ou", 
        team=t1_text, 
        max_concurrent_battles=n_battles, 
        server_configuration=SHOWDOWN_CONFIG, 
        account_configuration=AccountConfiguration(f"SimBot_A_{iteration%10}", None)
    )
    p2 = Gen9Bot(
        battle_format="gen9ou", 
        team=t2_text, 
        max_concurrent_battles=n_battles, 
        server_configuration=SHOWDOWN_CONFIG, 
        account_configuration=AccountConfiguration(f"SimBot_B_{iteration%10}", None)
    )

    start_time = time.time()
    try:
        await p1.battle_against(p2, n_battles=n_battles)
    except Exception as e:
        print(f"Error during battles: {e}")

    duration = time.time() - start_time
    
    total_played = p1.n_finished_battles
    p1_wins = p1.n_won_battles
    p2_wins = total_played - p1_wins
    
    print(f"--> Finished {total_played} battles in {duration:.2f}s")
    print(f"--> Result: Team A ({p1_wins}) vs Team B ({p2_wins})")

    if total_played > 0:
        db.update_stats(t1_hash, total_played, p1_wins)
        db.update_stats(t2_hash, total_played, p2_wins)
        print("--> Database Updated.")
    else:
        print("--> WARNING: No battles recorded.")

async def main_loop():
    iteration = 1
    print("--- Starting Infinite Simulation Loop ---")
    print("Press Ctrl+C to stop manually.")
    
    while True:
        await run_single_set(iteration)
        
        sleep_time = random.randint(5, 15)
        print(f"Sleeping for {sleep_time}s...")
        time.sleep(sleep_time)
        
        iteration += 1

if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main_loop())
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")