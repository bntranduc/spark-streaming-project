import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns

from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=2000, limit=100)

load_dotenv()

# Connection à la base de données
def get_database_connection():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD")
    )
    return conn

def close_database_connection(conn):
    conn.close

conn = get_database_connection()

# ---------- Fonctions utilitaires ----------
def get_business_by_id(id, conn):
    with conn.cursor() as cursor:
        query = "SELECT name FROM business_table WHERE business_id = %s"
        cursor.execute(query, (id,))
        return cursor.fetchone()[0]

def get_user_by_id(id, conn):
    with conn.cursor() as cursor:
        query = "SELECT * FROM user_table WHERE user_id = %s"
        cursor.execute(query, (id,))
        return cursor.fetchone()

def query_db(query, conn):
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Erreur lors de la requête : {e}")
        return pd.DataFrame()

# ---------- VISUALISATIONS ----------
# Interface Streamlit
st.title("📝 Dashboard Yelp")
col1, col2, col3 = st.columns(3)
#1 user nbr
query_user = """
SELECT * FROM user_table;
"""
all_user = query_db(query_user, conn)
col1.write("Nombre d'utilisateur :")
col1.write(all_user.shape[0])

#2 review nbr
query_review = """
SELECT * FROM review_table;
"""
all_review = query_db(query_user, conn)
col2.write("Nombre de review :")
col2.write(all_review.shape[0])

#3 business nbr
query_business = """
SELECT * FROM business_table;
"""
all_query_business = query_db(query_business, conn)
col3.write("Nombre d'entreprise :")
col3.write(all_query_business.shape[0])

# 1. top_fun_business_table
st.header("1. top_fun_business_table")
query1 = """
SELECT * FROM top_fun_business_table;
"""
df1 = query_db(query1, conn)
df1['name'] = df1.apply(lambda x: get_business_by_id(x['business_id'], conn), axis=1)
fig_1, ax = plt.subplots()
ax.bar(df1['name'], df1['total_useful'], color='skyblue')
ax.set_xlabel("Business")
ax.set_ylabel("Count")
ax.set_title("Top Fun Business")
st.pyplot(fig_1)


# 2. top_usefull_user_table
st.header("2. top_usefull_user_table")
query2 = """
SELECT * FROM top_usefull_user_table;
"""
df2 = query_db(query2, conn)
st.dataframe(df2)

# 3. Établissements les plus cool / fun / useful
st.header("3. top_faithful_user_table")
query3 = """
SELECT * FROM top_faithful_user_table;
"""
df3 = query_db(query3, conn)
st.dataframe(df3)

# 4. Taux de fermeture des établissements selon leur note
st.header("4. Taux de fermeture : note vs statut")
query4 = """
SELECT AVG(r.stars) AS average_stars, COUNT(r.review_id) AS review_count
FROM business_table b
JOIN review_table r ON b.business_id = r.business_id
WHERE b.is_open = 0;
"""
df4 = query_db(query4, conn)
st.dataframe(df4)

# 5. Évolution de l’activité sur Yelp
st.header("5. Évolution de l’activité (reviews, users, businesses)")
query5 = """
SELECT * FROM activity_evolution_table ORDER BY year_month;
"""
df5 = query_db(query5, conn)
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
df6 = query_db(query6, conn)
st.dataframe(df6)

# 7. Utilisateurs les plus populaires
st.header("7. Utilisateurs les plus populaires")
query7 = """
SELECT user_id, name, fans_count, friends_count, popularity_score
FROM top_popular_user_table
ORDER BY popularity_score DESC
LIMIT 20;
"""
df7 = query_db(query7, conn)
st.dataframe(df7)

# 8. Utilisateurs les plus fidèles par entreprise
st.header("8. Utilisateurs les plus fidèles")
query8 = """
SELECT * FROM top_faithful_user_table
ORDER BY rank ASC
LIMIT 20;
"""
df8 = query_db(query8, conn)
st.dataframe(df8)

# 9. Apex Predator Users
st.header("9. Apex Predator Users")
query9 = """
SELECT user_id, name, elite_years
FROM apex_predator_user_table
ORDER BY elite_years DESC
LIMIT 20;
"""
df9 = query_db(query9, conn)
st.dataframe(df9)

# 10. Impact du statut elite sur les notes
st.header("10. Impact du statut elite sur les notes")
query10 = """
SELECT * FROM elite_impact_on_rating_table;
"""
df10 = query_db(query10, conn)
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
