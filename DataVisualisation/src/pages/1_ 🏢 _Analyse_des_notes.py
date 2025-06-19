from database.getDataFromDatabase import *
import streamlit as st
import pandas as pd
import plotly.express as px
import re
import wordcloud
from wordcloud import STOPWORDS, WordCloud
import matplotlib.pyplot as plt

st.set_page_config(page_title="Yelp Dashboard – Analyse des avis",page_icon="📊",layout="wide",initial_sidebar_state="expanded")

st.markdown("# 📝 Analyse des Avis – Table `review_table`")
st.markdown("---")
st.markdown("## Objectif")
st.markdown("##### Analyser les notes laissées par les utilisateurs pour comprendre les raisons récurrentes des mauvaises évaluations des entreprises.")
st.markdown("## Axes d’analyse :")

st.markdown("### 1 - Distribution des notes")
with st.spinner("Chargement de la distribution des notes..."):
    try:
        distribution = query_db("SELECT * FROM review_distribution_table WHERE stars < 4;")
    except Exception as e:
        st.error("Impossible de charger les données depuis la base.")
        st.exception(e)
        distribution = None
if distribution is None or distribution.empty:
    st.info("Aucune donnée trouvée pour les notes. Veuillez vérifier la base.")
else:
    try:
        distribution = distribution.sort_values(by="stars")
        st.bar_chart(distribution.set_index("stars")["nb_notes"])
    except Exception as e:
        st.error("Une erreur est survenue lors de la génération du graphique.")
        st.exception(e)


st.markdown("#### 2 - Évolution des notes dans le temps (`date`)")
review_evolution = query_db("SELECT * FROM review_evolution_table;")
review_evolution["parsed_date"] = pd.to_datetime(review_evolution["parsed_date"])
fig = px.line(
    review_evolution,
    x="parsed_date",
    y="total_reviews",
    labels={"parsed_date": "Date", "total_reviews": "Nombre de reviews"},
)
fig.update_layout(
    xaxis_title="Date",
    yaxis_title="Nombre de reviews",
    template="plotly_white",
    hovermode="x unified"
)
st.plotly_chart(fig, use_container_width=True)


st.markdown("### 3 - Distribution des notes et leur utilité")
with st.spinner("Chargement des données..."):
    try:
        df = query_db("SELECT * FROM review_distribution_useful WHERE stars < 4;")
    except Exception as e:
        st.error("Erreur lors du chargement des données depuis la base.")
        st.exception(e)
        df = pd.DataFrame()
if df.empty:
    st.info("Aucune donnée disponible.")
else:
    try:
        df = df.sort_values(by="stars")
        fig, ax1 = plt.subplots(figsize=(8, 5))
        ax1.bar(df["stars"], df["nb_reviews"], color="#4c8bf5", label="Nombre d'avis", alpha=0.8)
        ax1.set_xlabel("Note")
        ax1.set_ylabel("Nombre d'avis", color="#4c8bf5")
        ax1.tick_params(axis="y", labelcolor="#4c8bf5")
        ax2 = ax1.twinx()
        ax2.plot(df["stars"], df["nb_useful"], color="#f59e0b", label="Total des votes 'useful'", linewidth=2, marker="o")
        ax2.set_ylabel("Utilité totale", color="#f59e0b")
        ax2.tick_params(axis="y", labelcolor="#f59e0b")
        fig.tight_layout()
        st.pyplot(fig)
    except Exception as e:
        st.error("Une erreur est survenue lors de l'affichage du graphique.")
        st.exception(e)


st.markdown("---")
st.markdown("### 4 - Mots les plus fréquents dans les avis 1★ a 3★")
with st.spinner("Analyse des avis en cours..."):
    try:
        review_df = query_db("SELECT text FROM review_table WHERE stars < 3;")
    except Exception as e:
        review_df = None
        st.error("Erreur lors du chargement des avis.")
        st.exception(e)
if review_df is None or review_df.empty:
    st.info("Aucun avis à 1★ ou 2★ disponible.")
else:
    try:
        all_text = " ".join(review_df['text'].dropna().tolist()).lower()
        all_text = re.sub(r"[^a-zA-ZÀ-ÿ\s]", "", all_text)
        all_text = re.sub(r"\s+", " ", all_text)
        wordcloud = WordCloud(
            width=800,
            height=400,
            background_color='white',
            stopwords=STOPWORDS.union({"restaurant", "place", "food"}),
            max_words=100,
            max_font_size=90,
            colormap="inferno",
            random_state=42
        ).generate(all_text)
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis('off')
        st.pyplot(fig)
    except Exception as e:
        st.error("Une erreur est survenue lors du traitement du texte.")
        st.exception(e)


st.markdown("---")
st.markdown("### Synthèse")
st.info(
    "Ces analyses permettent de détecter des tendances générales et des points problématiques récurrents dans les avis. Cela offre une base solide pour explorer les causes des mauvaises évaluations."
)
