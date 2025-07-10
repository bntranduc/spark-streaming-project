from database.getDataFromDatabase import *
import streamlit as st
import pandas as pd
import plotly.express as px
import re
import wordcloud
from wordcloud import STOPWORDS, WordCloud
import matplotlib.pyplot as plt

st.set_page_config(page_title="Yelp Dashboard – Analyse des avis",page_icon="📊",layout="wide",initial_sidebar_state="expanded")

st.markdown("# 📝 Analyse des Avis")
st.markdown("---")
st.markdown("### Objectif Général")
st.markdown("""
Comprendre **pourquoi certaines entreprises reçoivent de mauvaises évaluations** sur Yelp.  
Pour cela, nous avons structuré notre étude en plusieurs axes d'analyse visuelle et statistique.
""")

st.markdown("## Axes d’analyse :")
st.markdown("""
Voici les différentes dimensions analysées pour comprendre les causes des mauvaises évaluations sur Yelp :
1. **Distribution des notes**  
   - Mesurer la fréquence des mauvaises évaluations et identifier les notes les plus fréquentes.
2. **Saisonnalité des notes (par mois)**  
   - Détecter les périodes de l’année où les entreprises reçoivent plus de critiques négatives.
3. **Analyse hebdomadaire (par jour de la semaine)**  
   - Identifier les jours problématiques en termes de satisfaction client.
4. **Utilité perçue des avis négatifs**  
   - Vérifier si les avis critiques sont jugés pertinents par les autres utilisateurs.
5. **Analyse textuelle (nuage de mots)**  
   - Extraire les mots-clés récurrents dans les avis 1★ et 2★ pour détecter les motifs d'insatisfaction.
---
""")

st.markdown("---")
st.markdown("### 1 - Distribution des notes")
st.markdown("""
**Pourquoi ce graphique ?**
- Identifier la **fréquence des mauvaises notes (1★ à 3★)**.
- Mesurer **l’ampleur du problème**.
- Servir de **point de départ général** pour explorer les autres dimensions.
""")

with st.spinner("Chargement de la distribution des notes..."):
    try:
        distribution = query_db("SELECT * FROM review_distribution_table;")
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

        total_notes = distribution["nb_notes"].sum()
        bad_notes = distribution[distribution["stars"] < 4]["nb_notes"].sum()
        ratio = (bad_notes / total_notes) * 100

        st.markdown("""
        **Notes conservées pour l'étude** : Inférieur ou égale à 3★  
        **Nombre d'avis analysés** : `{} / {}` (**{:.2f}%** du total)
        """.format(bad_notes, total_notes, ratio))
        
    except Exception as e:
        st.error("Une erreur est survenue lors de la génération du graphique.")
        st.exception(e)

st.markdown("---")
st.markdown("### 1 - Moyenne des notes par mois (saisonnalité)")
st.markdown("""
**Pourquoi ce graphique ?**
- Détecter une **variabilité mensuelle** dans les notes.
- Hypothèse : des périodes de l’année peuvent affecter la qualité du service (vacances, météo, affluence...).
- Identifier des **pics ou baisses saisonnières** pour orienter les améliorations.
""")
with st.spinner("Chargement des données saisonnières..."):
    try:
        season_df = query_db("SELECT * FROM seasonal_review_stats WHERE avg_stars < 4;")
    except Exception as e:
        season_df = None
        st.error("Erreur lors du chargement des données.")
        st.exception(e)

if season_df is None or season_df.empty:
    st.info("Aucune donnée disponible.")
else:
    try:
        mois_order = ["January", "February", "March", "April", "May", "June",
                      "July", "August", "September", "October", "November", "December"]
        season_df['month_name'] = pd.Categorical(season_df['month_name'], categories=mois_order, ordered=True)
        season_df = season_df.sort_values("month_name")

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(season_df["month_name"], season_df["avg_stars"], marker='o', color="#2A9D8F")
        ax.set_title("Moyenne des notes par mois", fontsize=14)
        ax.set_ylabel("Note moyenne", fontsize=12)
        ax.set_xlabel("Mois", fontsize=12)
        ax.grid(True, linestyle="--", alpha=0.6)
        st.pyplot(fig)

    except Exception as e:
        st.error("Une erreur est survenue pendant l'affichage.")
        st.exception(e)

st.markdown("---")
st.markdown("### 2 - Moyenne des notes par jour de la semaine")
st.markdown("""
**Pourquoi ce graphique ?**
- Repérer les **jours problématiques** (ex. : surcharge le week-end, mauvaise organisation le lundi...).
- Hypothèse : certaines journées concentrent les mauvaises expériences.
- Permet de proposer des **actions opérationnelles ciblées**.
""")
with st.spinner("Chargement des données hebdomadaires..."):
    try:
        weekly_df = query_db("SELECT * FROM weekly_review_stats WHERE avg_stars < 4;")
    except Exception as e:
        weekly_df = None
        st.error("Erreur lors du chargement des données.")
        st.exception(e)

if weekly_df is None or weekly_df.empty:
    st.info("Aucune donnée disponible pour les jours de la semaine.")
else:
    try:
        jour_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        weekly_df['day_name'] = pd.Categorical(weekly_df['day_name'], categories=jour_order, ordered=True)
        weekly_df = weekly_df.sort_values("day_name")

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(weekly_df["day_name"], weekly_df["avg_stars"], color="#E76F51")
        ax.set_title("Note moyenne par jour de la semaine", fontsize=14)
        ax.set_ylabel("Note moyenne", fontsize=12)
        ax.set_xlabel("Jour de la semaine", fontsize=12)
        ax.grid(axis="y", linestyle="--", alpha=0.5)

        st.pyplot(fig)

    except Exception as e:
        st.error("Une erreur est survenue lors de l'affichage.")
        st.exception(e)

st.markdown("---")
st.markdown("### 3 - Distribution des notes vs nombre de votes 'useful'")
st.markdown("""
**Pourquoi ce graphique ?**
- Analyser **la pertinence perçue** des avis selon la note.
- Hypothèse : les mauvaises notes sont souvent **jugées utiles** par les autres utilisateurs → donc elles soulignent de vrais problèmes.
- Croise le **volume des avis négatifs** avec leur **crédibilité sociale**.
""")
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
st.markdown("""
**Pourquoi ce graphique ?**
- Extraire les **thèmes récurrents dans les avis très critiques**.
- Approche qualitative pour détecter les **sources concrètes d’insatisfaction** : service, attente, prix, propreté, etc.
- Donne une **vue synthétique du ressenti client**.
""")
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
st.markdown("""
Ces analyses croisées permettent de :
- Dégager des **tendances globales** (périodes ou jours à problème).
- Identifier les **raisons concrètes** exprimées dans les textes.
- Proposer des **axes d’amélioration** précis aux entreprises concernées.
""")
