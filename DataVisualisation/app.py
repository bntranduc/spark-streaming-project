import streamlit as st
import psycopg2
import pandas as pd

def get_data():
    try:
        conn = psycopg2.connect(
            host="postgres",         # ou "localhost" si tu testes hors Docker
            port=5432,
            dbname="mydatabase",
            user="user",
            password="password"
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
