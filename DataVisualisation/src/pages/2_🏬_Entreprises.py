from database.getDataFromDatabase import *
import streamlit as st
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("# 🏢 Analyse de la Table Entreprises – `business_table`")
st.markdown("---")
st.markdown("## Objectif")
st.markdown("##### Comprendre les caractéristiques des entreprises qui pourraient influencer leurs notes, notamment celles qui reçoivent des avis négatifs.")
st.markdown("## Axes d’analyse :")

st.markdown("### 1 - Catégories les plus associées aux mauvaises notes")

with st.spinner("Chargement des catégories..."):
    try:
        query_1 = "SELECT * FROM top_categories_by_rating WHERE rounded_rating = 1 ORDER BY nb_occurrences DESC LIMIT 10"
        query_2 = "SELECT * FROM top_categories_by_rating WHERE rounded_rating = 2 ORDER BY nb_occurrences DESC LIMIT 10"
        query_3 = "SELECT * FROM top_categories_by_rating WHERE rounded_rating = 3 ORDER BY nb_occurrences DESC LIMIT 10"

        df_1 = query_db(query_1)
        df_2 = query_db(query_2)
        df_3 = query_db(query_3)

    except Exception as e:
        st.error("Erreur lors de la récupération des données.")
        st.exception(e)
        df_1, df_2, df_3 = None, None, None

def draw_pie(df, rating_level):
    if df is not None and not df.empty:
        labels = df["category"]
        sizes = df["nb_occurrences"]
        colors = plt.cm.Pastel1.colors

        fig, ax = plt.subplots(figsize=(5, 5))
        wedges, texts, autotexts = ax.pie(
            sizes,
            labels=labels,
            autopct="%1.1f%%",
            startangle=140,
            colors=colors,
            textprops={"fontsize": 10},
            wedgeprops={"edgecolor": "white"}
        )
        ax.set_title(f"Catégories les plus fréquentes dans les avis {rating_level}★", fontsize=13)
        st.pyplot(fig)
    else:
        st.info(f"Aucune donnée disponible pour les avis {rating_level}★.")

col1, col2, col3 = st.columns(3)
with col1:
    draw_pie(df_1, 1)
with col2:
    draw_pie(df_2, 2)
with col3:
    draw_pie(df_3, 3)

st.markdown("---")

# 🔸 Localisation géographique
st.markdown("#### 📍 Localisation (`city`, `state`)")
st.markdown(
    "Analyser si certaines zones géographiques présentent plus de mauvaises notes, pour détecter des facteurs régionaux."
)
# 👉 METTRE UN GRAPHIQUE ICI (carte ou heatmap des notes par ville/état)

st.markdown("---")

# 🔸 Statut d’ouverture
st.markdown("#### 🚪 Statut d’ouverture (`is_open`)")
st.markdown(
    "Vérifier si les entreprises fermées reçoivent plus de mauvaises notes, ce qui pourrait indiquer des problèmes liés à la qualité."
)
# 👉 METTRE UN GRAPHIQUE ICI (comparaison notes moyennes entreprises ouvertes vs fermées)

st.markdown("---")

# 🔸 Ancienneté et volume d’avis
st.markdown("#### ⏳ Ancienneté et nombre d’avis")
st.markdown(
    "Étudier si les entreprises plus anciennes ou avec plus d’avis ont tendance à mieux ou moins bien être notées."
)
# 👉 METTRE UN GRAPHIQUE ICI (scatter plot ancienneté vs note moyenne, ou nb avis vs note)

st.markdown("---")

# 🧠 Synthèse
st.markdown("### 🧠 Synthèse")
st.info(
    "👉 Ces analyses aident à repérer les facteurs liés à l’entreprise qui influencent la satisfaction client, et à cibler les causes structurelles des mauvaises notes."
)
