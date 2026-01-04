import asyncio
import psycopg2
import logging
import sys
import os
import time
import random
import gc
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from poke_env import ServerConfiguration, AccountConfiguration
from poke_env.player import RandomPlayer
from teambuilder import TeamBuilder 

logging.basicConfig(level=logging.ERROR)
logging.getLogger("poke_env").setLevel(logging.ERROR) 
logging.getLogger("asyncio").setLevel(logging.ERROR)

DB_PARAMS = {"host": "postgres", "database": "airflow", "user": "airflow", "password": "airflow"}
SHOWDOWN_CONFIG = ServerConfiguration("ws://showdown:8000/showdown/websocket", None)

TIMEOUT_SECONDS = 300
MAX_CONCURRENT_BATTLES = 20 

def sanitize_team(team_text):
    """
    Firewall to replace illegal Gen9OU abilities/items with legal alternatives.
    """
    replacements = {
        "Ability: Sand Veil": "Ability: Rough Skin",
        "Ability: Snow Cloak": "Ability: Cursed Body",
        "Ability: Moody": "Ability: Inner Focus",
        "Ability: Arena Trap": "Ability: Sand Force",
        "Ability: Shadow Tag": "Ability: Frisk",
        "Ability: Magnet Pull": "Ability: Sturdy",
        "Item: King's Rock": "Item: Leftovers",
        "Item: Razor Fang": "Item: Life Orb",
        "Baton Pass": "U-turn",
        "Last Respects": "Shadow Ball",
        "Shed Tail": "Substitute",
    }
    for banned, legal in replacements.items():
        if banned in team_text:
            team_text = team_text.replace(banned, legal)
    return team_text

class DataHandler:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_PARAMS)
        self.cursor = self.conn.cursor()

    def register_team(self, team_hash, team_text):
        query = "INSERT INTO dim_teams (team_hash, team_text) VALUES (%s, %s) ON CONFLICT (team_hash) DO NOTHING"
        self.cursor.execute(query, (team_hash, team_text))
        self.conn.commit()

    def update_stats(self, team_hash, matches_played, wins_gained):
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
    
    def close(self):
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()

class Gen9Bot(RandomPlayer):
    def choose_move(self, battle):
        if battle.finished:
            return self.choose_random_move(battle)

        if battle.force_switch or not battle.active_pokemon:
            if battle.available_switches:
                opponent = battle.opponent_active_pokemon
                if opponent:
                    best_switch = max(
                        battle.available_switches, 
                        key=lambda p: p.base_stats['spe']
                    )
                    return self.create_order(best_switch)
                return self.create_order(battle.available_switches[0])
            return self.choose_random_move(battle)

        if battle.available_moves:
            opponent = battle.opponent_active_pokemon
            active = battle.active_pokemon
            
            immunity_abilities = {
                'levitate': 'GROUND', 'volt absorb': 'ELECTRIC', 'lightning rod': 'ELECTRIC',
                'motor drive': 'ELECTRIC', 'flash fire': 'FIRE', 'water absorb': 'WATER',
                'storm drain': 'WATER', 'dry skin': 'WATER', 'sap sipper': 'GRASS',
                'earth eater': 'GROUND', 'well-baked body': 'FIRE', 'purifying salt': 'GHOST'
            }
            setup_moves = ['swordsdance', 'dragondance', 'nastyplot', 'calmmind', 'shellsmash', 'quiverdance', 'bulkup', 'coil']
            hazards = ['stealthrock', 'spikes', 'toxicspikes', 'stickyweb']

            def get_move_score(move):
                score = move.base_power if move.base_power > 0 else 20
                
                if opponent:
                    score *= opponent.damage_multiplier(move)
                    
                    possible_abilities = [a.lower() for a in opponent.possible_abilities]
                    for ability, blocked_type in immunity_abilities.items():
                        if ability in possible_abilities and move.type.name == blocked_type:
                            score = 0
                            break
                
                if move.type in active.types:
                    score *= 1.5

                if move.id in setup_moves:
                    if active.current_hp_fraction > 0.8: 
                        score += 100 
                
                if move.id in hazards:
                    score += 60

                if opponent:
                    estimated_damage_percent = (move.base_power * 0.5) * opponent.damage_multiplier(move)
                    if estimated_damage_percent >= (opponent.current_hp_fraction * 100):
                        score += 500

                score *= random.uniform(0.95, 1.05)
                return score

            best_move = max(battle.available_moves, key=get_move_score)
            
            best_score = get_move_score(best_move)
            if best_score < 5 and battle.available_switches:
                 return self.create_order(random.choice(battle.available_switches))

            return self.create_order(best_move)

        return self.choose_random_move(battle)

async def run_single_set(iteration):
    n_battles = random.randint(100, 300) 
    run_id = random.randint(1000, 9999)
    
    print(f"\n=== Set #{iteration} (ID: {run_id}): Simulating {n_battles} Matches ===")
    
    builder = TeamBuilder()
    db = DataHandler()

    try:
        t1_text, t1_hash = builder.generate_showdown_team()
        t2_text, t2_hash = builder.generate_showdown_team()
        t1_text = sanitize_team(t1_text)
        t2_text = sanitize_team(t2_text)
        db.register_team(t1_hash, t1_text)
        db.register_team(t2_hash, t2_text)
        
        p1 = Gen9Bot(battle_format="gen9ou", team=t1_text, max_concurrent_battles=MAX_CONCURRENT_BATTLES, server_configuration=SHOWDOWN_CONFIG, account_configuration=AccountConfiguration(f"SimBot_A_{iteration}_{run_id}", None))
        p2 = Gen9Bot(battle_format="gen9ou", team=t2_text, max_concurrent_battles=MAX_CONCURRENT_BATTLES, server_configuration=SHOWDOWN_CONFIG, account_configuration=AccountConfiguration(f"SimBot_B_{iteration}_{run_id}", None))

        start_time = time.time()
        battle_task = asyncio.create_task(p1.battle_against(p2, n_battles=n_battles))
        
        while not battle_task.done():
            if time.time() - start_time > TIMEOUT_SECONDS:
                print(f"DEBUG: Set #{iteration} Timeout. Stopping.")
                battle_task.cancel()
                break
            await asyncio.sleep(5)

        try:
            await battle_task
        except: pass
        
        duration = time.time() - start_time
        total_played = p1.n_finished_battles
        print(f"--> Finished {total_played} battles in {duration:.2f}s")
        if total_played > 0:
            db.update_stats(t1_hash, total_played, p1.n_won_battles)
            db.update_stats(t2_hash, total_played, total_played - p1.n_won_battles)
            print("--> Database Updated.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        try:
            if 'p1' in locals(): await p1.stop_listening()
            if 'p2' in locals(): await p2.stop_listening()
        except: pass
        if 'db' in locals(): db.close()
        gc.collect()

async def main_loop():
    iteration = 1
    print("--- Starting Infinite Simulation Loop (Silent Mode) ---")
    while True:
        await run_single_set(iteration)
        time.sleep(random.randint(5, 10))
        iteration += 1

if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main_loop())
    except KeyboardInterrupt:
        print("\nSimulation stopped.")