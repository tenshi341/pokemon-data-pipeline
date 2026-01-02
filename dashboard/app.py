import streamlit as st #type:ignore
import psycopg2
import pandas as pd #type:ignore
import os

st.set_page_config(page_title="Pokemon Meta Tier List", layout="wide")

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "airflow"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow")
    )

def load_data():
    conn = get_connection()
    query = """
    SELECT 
        s.total_wins,
        s.total_matches,
        ROUND((s.total_wins::decimal / s.total_matches) * 100, 1) as win_rate,
        t.team_text,
        t.team_hash
    FROM fact_team_stats s
    JOIN dim_teams t ON s.team_hash = t.team_hash
    WHERE s.total_matches >= 10  -- Filter out teams with low sample size
    ORDER BY win_rate DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.title("🏆 Gen 9 OU Meta Tier List")
st.markdown("Automated simulation results from the Data Pipeline.")

if st.button("🔄 Refresh Data"):
    st.rerun()

try:
    df = load_data()
    
    search_query = st.text_input("🔍 Search for a Pokemon (e.g., 'Garchomp', 'Gliscor')", "")
    
    if search_query:
        df = df[df['team_text'].str.contains(search_query, case=False, na=False)]

    for index, row in df.iterrows():
        with st.container():
            col1, col2, col3 = st.columns([1, 4, 2])
            
            with col1:
                st.metric(label="Win Rate", value=f"{row['win_rate']}%")
                st.caption(f"{row['total_wins']}W - {row['total_matches'] - row['total_wins']}L")
            
            with col2:
                preview_lines = row['team_text'].split('\n')
                pokemon_names = [line.split('@')[0].strip() for line in preview_lines if '@' in line]
                st.subheader(" | ".join(pokemon_names[:3]))
                st.write(", ".join(pokemon_names))
            
            with col3:
                with st.expander("📋 View & Copy Build"):
                    st.code(row['team_text'], language='text')
            
            st.divider()

except Exception as e:
    st.error(f"Could not connect to database: {e}")