import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns

load_dotenv()

# def get_data():
#     try:
#         conn = psycopg2.connect(
#             host=os.getenv("POSTGRES_HOST", "localhost"),
#             port=os.getenv("POSTGRES_PORT", 5432),
#             dbname=os.getenv("DATABASE_NAME"),
#             user=os.getenv("DATABASE_USER"),
#             password=os.getenv("DATABASE_PASSWORD")
#         )
#         query = "SELECT review_id, user_id, business_id, stars, date FROM review_table LIMIT 20;"
#         df = pd.read_sql_query(query, conn)
#         conn.close()
#         return df
#     except Exception as e:
#         st.error(f"Erreur lors de la connexion à la base de données : {e}")
#         return pd.DataFrame()

# Connexion à la base
def query_db(query):
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", 5432),
            dbname=os.getenv("DATABASE_NAME"),
            user=os.getenv("DATABASE_USER"),
            password=os.getenv("DATABASE_PASSWORD")
        )
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erreur lors de la requête : {e}")
        return pd.DataFrame()

# Interface Streamlit
st.title("📝 Dashboard Yelp")

# 1. Les entreprises les plus notées et les mieux notées par catégorie
st.header("1. Entreprises les plus notées et mieux notées par catégorie")
query1 = """
SELECT 
    c.category,
    b.business_id,
    b.name,
    COUNT(r.review_id) AS review_count,
    AVG(r.stars) AS average_stars
FROM category_table c
JOIN business_table b ON c.business_id = b.business_id
JOIN review_table r ON r.business_id = b.business_id
WHERE c.category IS NOT NULL
GROUP BY c.category, b.business_id, b.name
ORDER BY review_count DESC
LIMIT 20;
"""
df1 = query_db(query1)
st.dataframe(df1)

# 2. Établissements les plus populaires par mois
st.header("2. Établissements les plus populaires par mois")
query2 = """
SELECT TO_CHAR(TO_DATE(date, 'YYYY-MM-DD'), 'YYYY-MM') AS year_month, business_id, COUNT(review_id) AS review_count
FROM review_table
GROUP BY year_month, business_id
ORDER BY year_month, review_count DESC
LIMIT 50;
"""
df2 = query_db(query2)
st.dataframe(df2)

# 3. Établissements les plus cool / fun / useful
st.header("3. Établissements les plus fun / useful / cool")
query3 = """
SELECT business_id,
       SUM(useful) AS total_useful,
       SUM(funny) AS total_funny,
       SUM(cool) AS total_cool
FROM review_table
GROUP BY business_id
ORDER BY total_useful DESC
LIMIT 20;
"""
df3 = query_db(query3)
st.dataframe(df3)

# 4. Taux de fermeture des établissements selon leur note
st.header("4. Taux de fermeture : note vs statut")
query4 = """
SELECT AVG(r.stars) AS average_stars, COUNT(r.review_id) AS review_count
FROM business_table b
JOIN review_table r ON b.business_id = r.business_id
WHERE b.is_open = 0;
"""
df4 = query_db(query4)
st.dataframe(df4)

# 5. Évolution de l’activité sur Yelp
st.header("5. Évolution de l’activité (reviews, users, businesses)")
query5 = """
SELECT * FROM activity_evolution_table ORDER BY year_month;
"""
df5 = query_db(query5)
if not df5.empty:
    df5['year_month'] = pd.to_datetime(df5['year_month'])
    st.line_chart(df5.set_index('year_month')[['reviews_count', 'users_count', 'business_count']])

# 6. Utilisateurs les plus influents
st.header("6. Utilisateurs les plus influents")
query6 = """
SELECT user_id, name, total_useful
FROM top_usefull_user_table
ORDER BY total_useful DESC
LIMIT 20;
"""
df6 = query_db(query6)
st.dataframe(df6)

# 7. Utilisateurs les plus populaires
st.header("7. Utilisateurs les plus populaires")
query7 = """
SELECT user_id, name, fans_count, friends_count, popularity_score
FROM top_popular_user_table
ORDER BY popularity_score DESC
LIMIT 20;
"""
df7 = query_db(query7)
st.dataframe(df7)

# 8. Utilisateurs les plus fidèles par entreprise
st.header("8. Utilisateurs les plus fidèles")
query8 = """
SELECT * FROM top_faithful_user_table
ORDER BY rank ASC
LIMIT 20;
"""
df8 = query_db(query8)
st.dataframe(df8)

# 9. Apex Predator Users
st.header("9. Apex Predator Users")
query9 = """
SELECT user_id, name, elite_years
FROM apex_predator_user_table
ORDER BY elite_years DESC
LIMIT 20;
"""
df9 = query_db(query9)
st.dataframe(df9)

# 10. Impact du statut elite sur les notes
st.header("10. Impact du statut elite sur les notes")
query10 = """
SELECT * FROM elite_impact_on_rating_table;
"""
df10 = query_db(query10)
st.dataframe(df10)

# Optionnel : diagramme comparatif
if not df10.empty:
    fig, ax = plt.subplots()
    sns.barplot(data=df10, x="elite_status", y="average_stars", ax=ax)
    ax.set_title("Évaluation moyenne par statut (elite vs non-elite)")
    st.pyplot(fig)

# df = get_data()
# if not df.empty:
#     st.dataframe(df)
# else:
#     st.warning("Aucune donnée disponible.")
