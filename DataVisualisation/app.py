import streamlit as st
import psycopg2
import pandas as pd
import os

def get_data():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", 5432),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        query = "SELECT review_id, user_id, business_id, stars, date FROM review_table LIMIT 20;"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erreur lors de la connexion à la base de données : {e}")
        return pd.DataFrame()

# Interface Streamlit
st.title("📝 Reviews Yelp")
st.write("Affichage des 20 premières reviews")

df = get_data()
if not df.empty:
    st.dataframe(df)
else:
    st.warning("Aucune donnée disponible.")
