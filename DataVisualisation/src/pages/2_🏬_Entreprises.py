from database.getDataFromDatabase import *
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px

st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("# 🏢 Analyse des Entreprises mal notés")
st.markdown("---")
st.markdown("## Objectif")
st.markdown("##### Comprendre les caractéristiques des entreprises qui pourraient influencer leurs notes, notamment celles qui reçoivent des avis négatifs.")
st.markdown("## Axes d’analyse :")
st.markdown("""
Voici les axes étudiés pour mieux comprendre les caractéristiques des entreprises recevant des avis négatifs :
1. **Catégories les plus associées aux mauvaises notes**  
   - Identifier les types d’activités les plus souvent mal notés (restauration, services, etc.)
2. **Répartition géographique des entreprises mal notées**  
   - Visualiser si certaines zones géographiques concentrent plus d’avis négatifs.
3. **Lien entre statut d’ouverture et mauvaise note**  
   - Vérifier si les entreprises fermées sont plus susceptibles d’avoir reçu de mauvaises évaluations.
---
""")
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
st.markdown("### 2 - Distribution de movaise par zone geographique")
with st.spinner("Chargement des catégories..."):
    try:
        query = "SELECT longitude, latitude FROM business_table WHERE rounded_rating < 4"
        business = query_db(query)
    except Exception as e:
        st.error("Erreur lors de la récupération des données.")
        st.exception(e)
        business = None

    if business is not None and not business.empty:
        business = business.dropna(subset=["longitude", "latitude"])
        st.map(business, latitude="latitude", longitude="longitude")
    else:
        st.info("Aucune donnée géographique disponible pour les entreprises mal notées.")

st.markdown("---")
st.markdown("### 3 - Note moyenne par statut d’ouverture")
st.markdown(
    "Vérifier si les entreprises fermées reçoivent plus de mauvaises notes, ce qui pourrait indiquer des problèmes liés à la qualité."
)

with st.spinner("Chargement des statistiques..."):
    try:
        query = "SELECT is_open, avg_rating, nbr_business FROM business_by_status_table"
        status_df = query_db(query)
    except Exception as e:
        st.error("Erreur lors de la récupération des données.")
        st.exception(e)
        status_df = None

    if status_df is not None and not status_df.empty:
        # Conversion pour affichage lisible
        status_df["Statut"] = status_df["is_open"].map({1: "Ouvertes", 0: "Fermées"})

        # Affichage de la moyenne des notes
        fig = px.bar(
            status_df,
            x="Statut",
            y="avg_rating",
            color="Statut",
            text="avg_rating",
            labels={"avg_rating": "Note moyenne"},
        )
        fig.update_layout(showlegend=False, yaxis_range=[0, 5])
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Aucune donnée disponible pour les notes par statut.")

st.markdown("---")

st.markdown("## Synthèse")
st.info(
    "Ces analyses aident à repérer les facteurs liés à l’entreprise qui influencent la satisfaction client, et à cibler les causes structurelles des mauvaises notes."
)
