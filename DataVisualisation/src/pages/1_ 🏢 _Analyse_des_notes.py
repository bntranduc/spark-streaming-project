from database.getDataFromDatabase import *
import streamlit as st
import pandas as pd
import plotly.express as px
import re
import wordcloud
from wordcloud import STOPWORDS, WordCloud
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("## 📝 Analyse des Avis – Table `review_table`")
st.markdown("---")

st.markdown("### 🎯 Objectif")
st.markdown("Analyser les notes laissées par les utilisateurs pour comprendre les raisons récurrentes des mauvaises évaluations des entreprises.")

st.markdown("### 🔍 Axes d’analyse proposés")

# 🔸 Distribution des notes


# Affichage du titre et de l'explication
st.markdown("### ⭐️ Distribution des notes (`stars`)")
st.markdown(
    "Visualiser la répartition des notes permet de détecter un éventuel déséquilibre "
    "(ex : beaucoup de 1★ ou 2★), ce qui peut indiquer une insatisfaction récurrente."
)

# Chargement des données
with st.spinner("⏳ Chargement de la distribution des notes..."):
    try:
        distribution = query_db("SELECT * FROM note_distribution_table;")
    except Exception as e:
        st.error("🚫 Impossible de charger les données depuis la base.")
        st.exception(e)
        distribution = None

# Affichage du graphique
if distribution is None or distribution.empty:
    st.info("📭 Aucune donnée trouvée pour les notes. Veuillez vérifier la base.")
else:
    try:
        # Tri des étoiles dans l'ordre croissant
        distribution = distribution.sort_values(by="stars")
        
        distribution = distribution.sort_values(by="stars")  # s'assurer que les étoiles soient dans l'ordre
        st.bar_chart(distribution.set_index("stars")["nb_notes"])

        # Résumé rapide
        total_avis = distribution['nb_notes'].sum()
        moyenne = round((distribution['stars'] * distribution['nb_notes']).sum() / total_avis, 2)

        st.markdown(f"📌 **Nombre total d'avis analysés :** `{total_avis}`")
        st.markdown(f"⭐️ **Note moyenne globale :** `{moyenne}` / 5")

    except Exception as e:
        st.error("❌ Une erreur est survenue lors de la génération du graphique.")
        st.exception(e)


st.markdown("---")

# 🔸 Évolution des notes dans le temps
st.markdown("#### 📆 Évolution des notes dans le temps (`date`)")
st.markdown(
    "Permet de détecter des périodes où la qualité a chuté (ex : changement de personnel, incident, etc.)."
)
# 👉 METTRE UN GRAPHIQUE ICI (courbe d’évolution ou heatmap temporelle)

review_evolution = query_db("SELECT * FROM review_evolution_table;")

review_evolution["parsed_date"] = pd.to_datetime(review_evolution["parsed_date"])

fig = px.line(
    review_evolution,
    x="parsed_date",
    y="total_reviews",
    labels={"parsed_date": "Date", "total_reviews": "Nombre de reviews"},
    title="Évolution des reviews dans le temps"
)

fig.update_layout(
    xaxis_title="Date",
    yaxis_title="Nombre de reviews",
    template="plotly_white",
    hovermode="x unified"
)

st.plotly_chart(fig, use_container_width=True)

review_evolution["cumulative_reviews"] = review_evolution["total_reviews"].cumsum()

fig_cum = px.line(
    review_evolution,
    x="parsed_date",
    y="cumulative_reviews",
    labels={"parsed_date": "Date", "cumulative_reviews": "Total cumulatif de reviews"},
    title="Croissance cumulative des reviews"
)

fig_cum.update_layout(
    xaxis_title="Date",
    yaxis_title="Total cumulatif de reviews",
    template="plotly_white",
    hovermode="x unified"
)

st.plotly_chart(fig_cum, use_container_width=True)


st.markdown("---")

# 🔸 Notes par utilité
st.markdown("#### 👍 Notes par utilité (`useful`)")
st.markdown(
    "Les avis jugés “utiles” par d'autres utilisateurs sont souvent plus pertinents et mettent en évidence les vrais problèmes."
)
# 👉 METTRE UN GRAPHIQUE ICI (boxplot ou scatter plot entre `stars` et `useful`)

st.markdown("---")

# 🔸 Analyse des mots fréquents dans les mauvaises notes
import re
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

st.markdown("### 🗣️ Mots les plus fréquents dans les avis 1★ et 2★")
st.markdown(
    """
    Les commentaires très négatifs peuvent révéler des problèmes récurrents.  
    Cette analyse textuelle permet d’identifier les **termes qui reviennent souvent dans les avis les plus critiques**.  
    > Exemples attendus : *attente*, *sale*, *cher*, *accueil*, etc.
    """
)

with st.spinner("🔎 Analyse des avis en cours..."):
    try:
        review_df = query_db("SELECT text FROM review_table WHERE stars < 3;")
    except Exception as e:
        review_df = None
        st.error("❌ Erreur lors du chargement des avis.")
        st.exception(e)

# Traitement s’il y a des données
if review_df is None or review_df.empty:
    st.info("📭 Aucun avis à 1★ ou 2★ disponible.")
else:
    try:
        # Concaténation de tous les textes
        all_text = " ".join(review_df['text'].dropna().tolist()).lower()
        
        # Nettoyage du texte
        all_text = re.sub(r"[^a-zA-ZÀ-ÿ\s]", "", all_text)  # support accents
        all_text = re.sub(r"\s+", " ", all_text)  # supprime les espaces multiples
        
        # Génération du nuage de mots
        wordcloud = WordCloud(
            width=800,
            height=400,
            background_color='white',
            stopwords=STOPWORDS.union({"restaurant", "place", "food"}),  # mots fréquents inutiles
            max_words=100,
            max_font_size=90,
            colormap="inferno",
            random_state=42
        ).generate(all_text)

        # Affichage du nuage
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis('off')
        st.pyplot(fig)

        st.caption("🔎 *Ce nuage de mots permet de visualiser rapidement les motifs de mécontentement les plus fréquents.*")

    except Exception as e:
        st.error("❌ Une erreur est survenue lors du traitement du texte.")
        st.exception(e)



st.markdown("---")

# 🔸 Notes par utilisateur

st.markdown("#### 👤 Notes données par les utilisateurs (`user_id`)")
st.markdown(
    """
    Certaines personnes ont tendance à attribuer souvent de mauvaises notes (1★ ou 2★), ce qui peut fausser l’analyse globale.  
    Identifier ces profils permet de mieux comprendre s’il s’agit de cas isolés ou d’un biais utilisateur.
    """
)

# Chargement des données depuis la table user_stats_table
with st.spinner("⏳ Chargement des statistiques utilisateur..."):
    try:
        user_stats = query_db("SELECT * FROM user_table;")
    except Exception as e:
        user_stats = None
        st.error("❌ Impossible de charger les données utilisateur.")
        st.exception(e)

# Vérification des données
if user_stats is None or user_stats.empty:
    st.info("📭 Aucune statistique utilisateur disponible.")
else:
    try:
        st.markdown("##### 📊 Distribution des moyennes de notes par utilisateur")

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.hist(user_stats["avg_stars"], bins=30, color="#2C8F9B", edgecolor='black')
        ax.set_title("Distribution des moyennes de notes par utilisateur", fontsize=14)
        ax.set_xlabel("Note moyenne", fontsize=12)
        ax.set_ylabel("Nombre d'utilisateurs", fontsize=12)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        st.pyplot(fig)

        st.markdown("##### 🔍 Exemple de profils d’utilisateurs très sévères")
        bad_raters = user_stats[user_stats["avg_stars"] < 2.5].sort_values(by="avg_stars")
        st.dataframe(bad_raters[["user_id", "avg_stars", "total_reviews"]].head(10))

    except Exception as e:
        st.error("🚫 Une erreur est survenue pendant l'affichage du graphique ou du tableau.")
        st.exception(e)

st.markdown("---")

# 🧠 Synthèse
st.markdown("### 🧠 Synthèse")
st.info(
    "👉 Ces analyses permettent de détecter des tendances générales et des points problématiques récurrents dans les avis. Cela offre une base solide pour explorer les causes des mauvaises évaluations."
)
