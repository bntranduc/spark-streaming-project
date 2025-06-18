from database.getDataFromDatabase import *
import streamlit as st

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
st.markdown("#### ⭐️ Distribution des notes (`stars`)")
st.markdown(
    "Cela permet de voir s’il existe un déséquilibre (ex : beaucoup de 1★ et 2★), ce qui suggère une insatisfaction fréquente."
)
# 👉 METTRE UN GRAPHIQUE ICI (Histogramme ou Countplot)
# ex : st.bar_chart(distribution)

st.markdown("---")

# 🔸 Évolution des notes dans le temps
st.markdown("#### 📆 Évolution des notes dans le temps (`date`)")
st.markdown(
    "Permet de détecter des périodes où la qualité a chuté (ex : changement de personnel, incident, etc.)."
)
# 👉 METTRE UN GRAPHIQUE ICI (courbe d’évolution ou heatmap temporelle)

st.markdown("---")

# 🔸 Notes par utilité
st.markdown("#### 👍 Notes par utilité (`useful`)")
st.markdown(
    "Les avis jugés “utiles” par d'autres utilisateurs sont souvent plus pertinents et mettent en évidence les vrais problèmes."
)
# 👉 METTRE UN GRAPHIQUE ICI (boxplot ou scatter plot entre `stars` et `useful`)

st.markdown("---")

# 🔸 Analyse des mots fréquents dans les mauvaises notes
st.markdown("#### 🗣️ Mots fréquents dans les avis 1★ et 2★")
st.markdown(
    "Une analyse simple des mots les plus utilisés dans les mauvais avis permet de repérer les problèmes récurrents (ex : *attente*, *sale*, *accueil*...)."
)
# 👉 METTRE UN NUAGE DE MOTS (Wordcloud) OU UN BAR CHART DES MOTS CLÉS

st.markdown("---")

# 🔸 Notes par utilisateur
st.markdown("#### 👤 Notes données par les utilisateurs (`user_id`)")
st.markdown(
    "Certaines personnes donnent souvent des mauvaises notes. Cela peut fausser les résultats, il faut identifier les profils biaisés."
)
# 👉 METTRE UN GRAPHIQUE ICI (histogramme des moyennes par utilisateur)

st.markdown("---")

# 🧠 Synthèse
st.markdown("### 🧠 Synthèse")
st.info(
    "👉 Ces analyses permettent de détecter des tendances générales et des points problématiques récurrents dans les avis. Cela offre une base solide pour explorer les causes des mauvaises évaluations."
)
