import pandas as pd #type:ignore
import hashlib
from sqlalchemy import create_engine #type:ignore
from poke_env.data import GenData

DB_CONNECTION = "postgresql://airflow:airflow@postgres/airflow"

class TeamBuilder:
    def __init__(self):
        self.engine = create_engine(DB_CONNECTION)
        self.pokedex = GenData.from_gen(9).pokedex

    def get_meta_stats(self):
        query = "SELECT * FROM fact_pokemon_usage ORDER BY usage_percent DESC LIMIT 100;"
        return pd.read_sql(query, self.engine)

    def get_valid_ability(self, pokemon_name):
        poke_id = pokemon_name.lower().replace(" ", "").replace("-", "").replace(".", "").replace("'", "")
        if "zamazenta" in poke_id: poke_id = "zamazenta"
        if poke_id in self.pokedex:
            return self.pokedex[poke_id].get('abilities', {}).get('0', 'Pressure')
        return "Pressure"

    def guess_evs(self, moves, pokemon_name):
        special_indicators = ['shadowball', 'thunderbolt', 'icebeam', 'flamethrower', 'hydropump', 'earthpower', 'moonblast', 'dracometeor', 'sludgebomb', 'makeitrain']
        is_special = any(m.lower().replace('-', '').replace(' ', '') in special_indicators for m in moves if m)
        return "EVs: 4 HP / 252 SpA / 252 Spe" if is_special else "EVs: 4 HP / 252 Atk / 252 Spe"

    def generate_showdown_team(self):
        """
        Returns a tuple: (team_text, team_hash)
        The hash is generated from a SORTED list of the pokemon builds to ensure uniqueness.
        """
        df = self.get_meta_stats()
        if df.empty: raise ValueError("No data found!")

        team_df = df.sample(n=6, weights=df['usage_percent'])
        
        pokemon_list = []
        
        for _, row in team_df.iterrows():
            p = {}
            p['name'] = row['pokemon_name']
            p['item'] = row.get('most_used_item') or "Leftovers"
            p['ability'] = row.get('most_common_ability') or self.get_valid_ability(p['name'])
            p['tera'] = row.get('most_common_tera')
            p['nature'] = row.get('most_common_nature') or "Serious"
            
            raw_moves = [row.get(f'move_{i}') for i in range(1, 5)]
            p['moves'] = [m for m in raw_moves if m and not pd.isna(m)]
            defaults = ['Protect', 'Substitute', 'Toxic', 'Rest']
            while len(p['moves']) < 4: p['moves'].append(defaults.pop(0))
            
            p['evs'] = self.guess_evs(p['moves'], p['name'])
            
            poke_str = f"{p['name']} @ {p['item']}\nAbility: {p['ability']}\n"
            if p['tera']: poke_str += f"Tera Type: {p['tera']}\n"
            poke_str += f"{p['evs']}\n{p['nature']} Nature\n"
            for move in p['moves']: poke_str += f"- {move}\n"
            
            pokemon_list.append(poke_str.strip())

        pokemon_list.sort()
        
        final_team_text = "\n\n".join(pokemon_list)
        
        team_hash = hashlib.md5(final_team_text.encode('utf-8')).hexdigest()
        
        return final_team_text, team_hash

if __name__ == "__main__":
    builder = TeamBuilder()
    text, h = builder.generate_showdown_team()
    print(f"Hash: {h}")
    print(text)